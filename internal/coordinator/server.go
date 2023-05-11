/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	multiclusterv1alpha1 "github.com/aryan9600/multi-cluster-flagger/api/v1alpha1"
	flaggerv1 "github.com/fluxcd/flagger/pkg/apis/flagger/v1beta1"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

const RetryKey = "multicluster.flagger.app/retry"

type Coordinator struct {
	port string
	client.Client
	logger *zap.SugaredLogger
}

func NewCoordinator(port string, c client.Client, logger *zap.SugaredLogger) *Coordinator {
	return &Coordinator{
		port:   port,
		Client: c,
		logger: logger,
	}
}

func (c *Coordinator) ListenAndServe(stopCh <-chan struct{}) {
	mux := http.DefaultServeMux
	mux.HandleFunc("/confirm-rollout", func(w http.ResponseWriter, r *http.Request) {
		c.ConfirmRollout(w, r)
	})
	mux.HandleFunc("/confirm-promotion", func(w http.ResponseWriter, r *http.Request) {
		c.ConfirmPromotion(w, r)
	})
	mux.HandleFunc("/post-rollout", func(w http.ResponseWriter, r *http.Request) {
		c.PostRollout(w, r)
	})
	mux.HandleFunc("/rollback", func(w http.ResponseWriter, r *http.Request) {
		c.Rollback(w, r)
	})

	srv := &http.Server{
		Addr:    ":" + c.port,
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			c.logger.Error(err, "Coordinator server crashed")
			os.Exit(1)
		}
	}()

	// wait for SIGTERM or SIGINT
	<-stopCh
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		c.logger.Error(err, "Coordinator server graceful shutdown failed")
	} else {
		c.logger.Info("Coordinator server stopped")
	}

}

func (c *Coordinator) ConfirmRollout(w http.ResponseWriter, r *http.Request) {
	payload, err := getWebhookPayload(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	clusterName := payload.Metadata["clusterName"]
	clusterNamespace := payload.Metadata["clusterNamespace"]
	logger := c.logger.With("leaf cluster name", clusterName, "leaf cluster namespace", clusterNamespace, "webhook", "confirm-rollout")

	mccObj, err := c.getMultiClusterCanary(payload.Name, payload.Metadata["mccNamespace"])
	if err != nil {
		logger.Errorf("could not find matching mcc %s/%s: %w", payload.Name, payload.Metadata["mccNamespace"], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logger.Debugf("found matching mcc obj: %s/%s", mccObj.Namespace, mccObj.Name)

	if mccObj.Status.Phase == multiclusterv1alpha1.Promoted ||
		mccObj.Status.Phase == multiclusterv1alpha1.RolledBack ||
		mccObj.Status.Phase == "" {
		mccObj.Status.Phase = multiclusterv1alpha1.PendingRolloutApproval
		if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
			logger.Errorf("could not update mcc %s/%s phase to %s: %w", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		logger.Infof("updated mcc %s/%s phase to %s", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase)
	}

	startRollout := true
	foundCanary := false
	var approvedCanaries int
	for i, item := range mccObj.Status.Inevntory {
		if item.ClusterName == clusterName && item.ClusterNamespace == clusterNamespace {
			foundCanary = true
			item.State = multiclusterv1alpha1.RolloutApproved
			mccObj.Status.Inevntory[i] = item
			if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
				logger.Errorf("could not update mcc %s/%s status: %w", mccObj.Namespace, mccObj.Name, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			logger.Infof("updated mcc %s/%s status", mccObj.Namespace, mccObj.Name)
			// if this canary is being retried, we don't need to wait for other
			// canaries' rollout to be apporved.
			if item.Retries > 0 {
				logger.Infof("canary %s.%s is being retried, allowing for rollout to start", item.Name, item.Namespace)
				startRollout = true
				break
			}
		}
		if item.State != multiclusterv1alpha1.RolloutApproved {
			startRollout = false
		} else {
			approvedCanaries += 1
		}
	}

	if !foundCanary {
		logger.Errorf("could not find a matching canary in mcc %s/%s", mccObj.Namespace, mccObj.Name)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// If the rollout is pending approval but we are still waiting for some Canaries
	// to ask for approval, compare the time we first approved this rollout to
	// the current time. If its more than the allowed timeout, then approve
	// the rollout anyway.
	if mccObj.Status.Phase == multiclusterv1alpha1.PendingRolloutApproval &&
		mccObj.Status.RolloutApprovedAt != nil &&
		metav1.Now().Sub(mccObj.Status.RolloutApprovedAt.Time) > mccObj.Spec.Timeout.Duration {
		startRollout = true
	}

	// Record the time at which we approved the rollout for this particular
	// set of Canaries.
	if approvedCanaries == 1 && mccObj.Status.RolloutApprovedAt == nil {
		now := metav1.Now()
		mccObj.Status.RolloutApprovedAt = &now
		if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
			logger.Errorf("could not update mcc %s/%s status: %w", mccObj.Namespace, mccObj.Name, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		logger.Infof("updated mcc %s/%s status.rolloutApprovedAt: %v", mccObj.Namespace, mccObj.Name, mccObj.Status.RolloutApprovedAt)
	}

	if startRollout {
		mccObj.Status.Phase = multiclusterv1alpha1.Progressing
		if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
			logger.Errorf("could not update mcc %s/%s phase to %s: %w", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		logger.Infof("updated mcc %s/%s phase to %s", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase)
		w.WriteHeader(http.StatusOK)
	} else {
		logger.Debug("waiting for other canaries' rollout to be approved")
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (c *Coordinator) ConfirmPromotion(w http.ResponseWriter, r *http.Request) {
	payload, err := getWebhookPayload(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	clusterName := payload.Metadata["clusterName"]
	clusterNamespace := payload.Metadata["clusterNamespace"]
	logger := c.logger.With("leaf cluster name", clusterName, "leaf cluster namespace", clusterNamespace, "webhook", "confirm-promotion")

	mccObj, err := c.getMultiClusterCanary(payload.Name, payload.Metadata["mccNamespace"])
	if err != nil {
		logger.Errorf("could not find matching mcc %s/%s: %w", payload.Name, payload.Metadata["mccNamespace"], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logger.Debugf("found matching mcc obj: %s/%s", mccObj.Namespace, mccObj.Name)

	if mccObj.Status.Phase == multiclusterv1alpha1.Progressing {
		mccObj.Status.Phase = multiclusterv1alpha1.PendingPromotionApproval
		if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
			logger.Errorf("could not update mcc %s/%s phase to %s: %w", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		logger.Infof("updated mcc %s/%s phase to %s", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase)
	}

	shouldBePromoted := true
	foundCanary := false
	for i, item := range mccObj.Status.Inevntory {
		if item.ClusterName == clusterName && item.ClusterNamespace == clusterNamespace {
			foundCanary = true
			item.State = multiclusterv1alpha1.PromotionApproved
			mccObj.Status.Inevntory[i] = item
			if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
				logger.Errorf("could not update mcc %s/%s status: %w", mccObj.Namespace, mccObj.Name, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			logger.Infof("updated mcc %s/%s status", mccObj.Namespace, mccObj.Name)
		}
		if item.State != multiclusterv1alpha1.PromotionApproved {
			shouldBePromoted = false
		}
	}

	if !foundCanary {
		logger.Errorf("could not find a matching canary in mcc %s/%s", mccObj.Namespace, mccObj.Name)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !shouldBePromoted {
		logger.Debug("waiting for other canaries' promotion to be approved")
		w.WriteHeader(http.StatusBadRequest)
	} else {
		mccObj.Status.Phase = multiclusterv1alpha1.Promoted
		mccObj.Status.RolloutApprovedAt = nil
		if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
			logger.Errorf("could not update mcc %s/%s phase to %s: %w", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		logger.Infof("updated mcc %s/%s phase to %s", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase)
		w.WriteHeader(http.StatusOK)
	}
}

func (c *Coordinator) PostRollout(w http.ResponseWriter, r *http.Request) {
	payload, err := getWebhookPayload(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	clusterName := payload.Metadata["clusterName"]
	clusterNamespace := payload.Metadata["clusterNamespace"]
	logger := c.logger.With("leaf cluster name", clusterName, "leaf cluster namespace", clusterNamespace, "webhook", "post-rollout")

	mccObj, err := c.getMultiClusterCanary(payload.Name, payload.Metadata["mccNamespace"])
	if err != nil {
		logger.Errorf("could not find matching mcc %s/%s: %w", payload.Name, payload.Metadata["mccNamespace"], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logger.Debugf("found matching mcc obj: %s/%s", mccObj.Namespace, mccObj.Name)

	// We don't need to do anything if we are the ones who approved the rollback.
	if payload.Phase == flaggerv1.CanaryPhaseFailed && mccObj.Status.Phase != multiclusterv1alpha1.RolledBack {
		if mccObj.Spec.PromotionStrategy.Type == "strict" {
			// mark the phase as PendingRollback because we want to rollback ALL canaries.
			mccObj.Status.Phase = multiclusterv1alpha1.PendingRollback

			err := retry.OnError(retry.DefaultRetry, func(err error) bool {
				return err != nil
			}, func() error {
				return c.Client.Status().Update(context.TODO(), mccObj)
			})
			if err != nil {
				logger.Errorf("could not update mcc %s/%s phase to %s: %w", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			logger.Infof("updated mcc %s/%s phase to %s", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase)
		} else if mccObj.Spec.PromotionStrategy.Type == "pragmatic" {
			foundCanary := false
			for i, item := range mccObj.Status.Inevntory {
				if item.ClusterName == clusterName && item.ClusterNamespace == clusterNamespace {
					foundCanary = true
					// if we have exhausted the allowed no. of retries, then do a full rollback
					if item.Retries == mccObj.GetFailedRetriesThreshold() {
						item.Retries = 0
						item.State = multiclusterv1alpha1.Failed
						mccObj.Status.Phase = multiclusterv1alpha1.PendingRollback
					} else {
						logger.Infof("retrying canary %s/%s in cluster %s/%s",
							payload.Namespace, payload.Name, clusterNamespace, clusterName)

						if err := c.retryCanary(clusterName, clusterNamespace,
							mccObj.Spec.TargetRef.Name, payload.Namespace); err != nil {
							logger.Errorf("could not update deployment %s/%s retry annotation in %s/%s: %w",
								mccObj.Spec.TargetRef.Name, payload.Namespace, clusterName, clusterNamespace, err)
							w.WriteHeader(http.StatusInternalServerError)
							return
						}
						item.Retries += 1
						item.State = multiclusterv1alpha1.Retrying
					}

					mccObj.Status.Inevntory[i] = item
					if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
						logger.Errorf("could not update mcc %s/%s status: %w", mccObj.Namespace, mccObj.Name, err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
				}
			}

			if !foundCanary {
				logger.Errorf("could not find a matching canary in mcc %s/%s", mccObj.Namespace, mccObj.Name)
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
	} else if payload.Phase == flaggerv1.CanaryPhaseSucceeded {
		// mark the canary obj as succeeded
		for i, item := range mccObj.Status.Inevntory {
			if item.ClusterName == clusterName && item.ClusterNamespace == clusterNamespace {
				item.State = multiclusterv1alpha1.Succeeded
				mccObj.Status.Inevntory[i] = item
				if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
					logger.Errorf("could not update mcc %s/%s status: %w", mccObj.Namespace, mccObj.Name, err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

// retryCanary builds a client for the required cluster and uses that to
// update the target Deployment's pod's annotation to force a retry of the Canary.
func (c *Coordinator) retryCanary(clusterName, clusterNamespace, deploymentName, deploymentNamespace string) error {
	clusterKey := types.NamespacedName{
		Namespace: clusterNamespace,
		Name:      clusterName,
	}
	clusterObj := &multiclusterv1alpha1.GitopsCluster{}
	if err := c.Get(context.TODO(), clusterKey, clusterObj); err != nil {
		return err
	}
	kubeClient, err := c.getClientForCluster(clusterObj)
	if err != nil {
		return err
	}

	deploymentKey := types.NamespacedName{
		Namespace: deploymentNamespace,
		Name:      deploymentName,
	}
	if err = retry.OnError(retry.DefaultRetry, func(err error) bool {
		return err != nil
	}, func() error {
		deployment := &appsv1.Deployment{}
		if err = kubeClient.Get(context.TODO(), deploymentKey, deployment); err != nil {
			return err
		}
		if deployment.Spec.Template.Annotations == nil {
			deployment.Spec.Template.Annotations = map[string]string{}
		}
		// add an annotation to the pod template to trigger a retry
		deployment.Spec.Template.Annotations[RetryKey] = time.Now().Format(time.RFC3339Nano)
		return kubeClient.Update(context.TODO(), deployment)
	}); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) Rollback(w http.ResponseWriter, r *http.Request) {
	payload, err := getWebhookPayload(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	clusterName := payload.Metadata["clusterName"]
	clusterNamespace := payload.Metadata["clusterNamespace"]
	logger := c.logger.With("leaf cluster name", clusterName, "leaf cluster namespace", clusterNamespace, "path", "rollback")

	mccObj, err := c.getMultiClusterCanary(payload.Name, payload.Metadata["mccNamespace"])
	if err != nil {
		logger.Errorf("could not find matching mcc %s/%s: %w", payload.Name, payload.Metadata["mccNamespace"], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logger.Debugf("found matching mcc obj: %s/%s", mccObj.Namespace, mccObj.Name)

	// If the object is pending rollback or has been marked as rolled back, then we approve the rollback of all Canaries
	// and mark them as failed.
	if mccObj.Status.Phase == multiclusterv1alpha1.PendingRollback || mccObj.Status.Phase == multiclusterv1alpha1.RolledBack {
		foundCanary := false
		for i, item := range mccObj.Status.Inevntory {
			if item.ClusterName == clusterName && item.ClusterNamespace == clusterNamespace {
				foundCanary = true
				item.State = multiclusterv1alpha1.Failed
				mccObj.Status.Inevntory[i] = item
			}
		}
		if !foundCanary {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		mccObj.Status.Phase = multiclusterv1alpha1.RolledBack
		mccObj.Status.RolloutApprovedAt = nil
		if err = c.Client.Status().Update(context.TODO(), mccObj); err != nil {
			logger.Errorf("could not update mcc %s/%s phase to %s: %w", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		logger.Infof("updated mcc %s/%s phase to %s", mccObj.Namespace, mccObj.Name, mccObj.Status.Phase)
		w.WriteHeader(http.StatusOK)
	} else {
		logger.Debugf("skipping rollback for mcc %s/%s", mccObj.Namespace, mccObj.Name)
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (c *Coordinator) getClientForCluster(gitopsClusterObj *multiclusterv1alpha1.GitopsCluster) (client.Client, error) {
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: gitopsClusterObj.Namespace,
		Name:      gitopsClusterObj.Spec.SecretRef.Name,
	}
	if err := c.Get(context.TODO(), secretKey, secret); err != nil {
		return nil, fmt.Errorf("unable to get secret %s: %w", secretKey, err)
	}

	var kubeConfig []byte
	switch {
	case secret.Data["value"] != nil:
		kubeConfig = secret.Data["value"]
	case secret.Data["value.yaml"] != nil:
		kubeConfig = secret.Data["value.yaml"]
	default:
		return nil, fmt.Errorf("KubeConfig secret '%s' does not contain a 'value' key with a kubeconfig", secret.Name)
	}

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to construct rest config: %w", err)
	}

	restMapper, err := apiutil.NewDynamicRESTMapper(restConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to construct rest mapper: %w", err)
	}

	kubeClient, err := client.New(restConfig, client.Options{
		Scheme: c.Client.Scheme(),
		Mapper: restMapper,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to construct kube client: %w", err)
	}

	return kubeClient, err
}

func getWebhookPayload(reader io.Reader) (*flaggerv1.CanaryWebhookPayload, error) {
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	payload := &flaggerv1.CanaryWebhookPayload{}
	if err = json.Unmarshal(body, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *Coordinator) getMultiClusterCanary(name, namespace string) (*multiclusterv1alpha1.MultiClusterCanary, error) {
	mccKey := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}
	mccObj := &multiclusterv1alpha1.MultiClusterCanary{}
	if err := c.Client.Get(context.TODO(), mccKey, mccObj); err != nil {
		return nil, err
	}
	return mccObj, nil
}
