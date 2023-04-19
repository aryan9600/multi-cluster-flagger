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

package controller

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	multiclusterv1alpha1 "github.com/aryan9600/multi-cluster-flagger/api/v1alpha1"
	flaggerv1 "github.com/fluxcd/flagger/pkg/apis/flagger/v1beta1"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MultiClusterCanaryReconciler reconciles a MultiClusterCanary object
type MultiClusterCanaryReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Host   string
}

//+kubebuilder:rbac:groups=multicluster.flagger.app,resources=multiclustercanaries,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=multicluster.flagger.app,resources=multiclustercanaries/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=multicluster.flagger.app,resources=multiclustercanaries/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *MultiClusterCanaryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	mcc := &multiclusterv1alpha1.MultiClusterCanary{}
	if err := r.Get(ctx, req.NamespacedName, mcc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	clusterSelector, err := metav1.LabelSelectorAsSelector(&mcc.Spec.GitopsClusterSelector)
	if err != nil {
		return ctrl.Result{Requeue: true}, err
	}
	gitopsClusters := &multiclusterv1alpha1.GitopsClusterList{}
	if err := r.List(ctx, gitopsClusters, client.MatchingLabelsSelector{Selector: clusterSelector}); err != nil {
		return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
	}

	var clusterKeys []string
	for _, cluster := range gitopsClusters.Items {
		clusterKeys = append(clusterKeys, fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name))
	}

	var desiredCanaryRefs []multiclusterv1alpha1.CanaryObjRef
	for _, cluster := range gitopsClusters.Items {
		desiredCanaryRefs = append(desiredCanaryRefs, multiclusterv1alpha1.CanaryObjRef{
			ClusterName:      cluster.Name,
			ClusterNamespace: cluster.Namespace,
			Name:             mcc.Name,
			Namespace:        mcc.GetCanaryNamespace(),
		})
	}

	if len(mcc.Status.Inevntory) > 0 {
		var presentCanaryRefs []multiclusterv1alpha1.CanaryObjRef
		for _, item := range mcc.Status.Inevntory {
			presentCanaryRefs = append(presentCanaryRefs, multiclusterv1alpha1.CanaryObjRef{
				ClusterName:      item.ClusterName,
				ClusterNamespace: item.ClusterNamespace,
				Name:             item.Name,
				Namespace:        item.Namespace,
			})
		}
		staleCanaryRefs := diff(presentCanaryRefs, desiredCanaryRefs)
		for _, staleCanaryRef := range staleCanaryRefs {
			clusterKey := types.NamespacedName{
				Name:      staleCanaryRef.ClusterName,
				Namespace: staleCanaryRef.ClusterNamespace,
			}
			clusterObj := &multiclusterv1alpha1.GitopsCluster{}
			if err := r.Get(ctx, clusterKey, clusterObj); err != nil {
				log.Error(err, "could not fetch gitops cluster", "key", clusterKey)
				return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
			}

			kubeClient, err := r.getClientForCluster(ctx, clusterObj)
			if err != nil {
				return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
			}

			staleCanaryObj := &flaggerv1.Canary{
				ObjectMeta: metav1.ObjectMeta{
					Name:      staleCanaryRef.Name,
					Namespace: staleCanaryRef.Namespace,
				},
			}
			if err = kubeClient.Delete(ctx, staleCanaryObj, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
			}

			log.Info("deleted stale canary successfully", "key", staleCanaryRef)
			for i, item := range mcc.Status.Inevntory {
				if item.ClusterName == staleCanaryRef.ClusterName &&
					item.ClusterNamespace == staleCanaryRef.ClusterNamespace &&
					item.Name == staleCanaryRef.Name && item.Namespace == staleCanaryRef.Namespace {
					mcc.Status.Inevntory = remove(mcc.Status.Inevntory, i)
				}
			}
		}
		// if err = r.Status().Update(ctx, mcc); err != nil {
		// return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()},
		// fmt.Errorf("failed to update status: %w", err)
		// }
	}

	webhooks := []flaggerv1.CanaryWebhook{
		{
			Name: "multi-cluster-flagger-confirm-rollout",
			Type: "confirm-rollout",
			URL:  fmt.Sprintf("%s/confirm-rollout", r.Host),
		},
		{
			Name: "multi-cluster-flagger-confirm-promotion",
			Type: "confirm-promotion",
			URL:  fmt.Sprintf("%s/confirm-promotion", r.Host),
		},
		{
			Name: "multi-cluster-flagger-post-rollout",
			Type: "post-rollout",
			URL:  fmt.Sprintf("%s/post-rollout", r.Host),
		},
		{
			Name: "multi-cluster-flagger-rollback",
			Type: "rollback",
			URL:  fmt.Sprintf("%s/rollback", r.Host),
		},
	}
	mcc.Spec.Analysis.CanaryAnalysis.Webhooks = append(mcc.Spec.Analysis.CanaryAnalysis.Webhooks, webhooks...)

	commonCanarySpec := flaggerv1.CanarySpec{
		TargetRef:     mcc.Spec.TargetRef,
		AutoscalerRef: mcc.Spec.AutoscalerRef,
		UpstreamRef:   mcc.Spec.UpstreamRef,
		IngressRef:    mcc.Spec.IngressRef,
		RouteRef:      mcc.Spec.RouteRef,
		Service:       mcc.Spec.Service.CanaryService,
		Analysis:      &mcc.Spec.Analysis.CanaryAnalysis,
	}

	for _, desiredCanaryRef := range desiredCanaryRefs {
		cluster := &multiclusterv1alpha1.GitopsCluster{}
		clusterKey := types.NamespacedName{
			Namespace: desiredCanaryRef.ClusterNamespace,
			Name:      desiredCanaryRef.ClusterName,
		}
		if err := r.Client.Get(ctx, clusterKey, cluster); err != nil {
			return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
		}
		kubeClient, err := r.getClientForCluster(ctx, cluster)
		if err != nil {
			return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
		}

		canarySpec := commonCanarySpec
		for i, webhhok := range canarySpec.Analysis.Webhooks {
			if strings.HasPrefix(webhhok.Name, "multi-cluster-flagger") {
				metadata := make(map[string]string, 0)
				if webhhok.Metadata != nil {
					metadata = *webhhok.Metadata
				}
				metadata["mccNamespace"] = mcc.Namespace
				metadata["clusterNamespace"] = desiredCanaryRef.ClusterNamespace
				metadata["clusterName"] = desiredCanaryRef.ClusterName
				webhhok.Metadata = &metadata
				canarySpec.Analysis.Webhooks[i] = webhhok
			}
		}

		canaryKey := types.NamespacedName{
			Namespace: desiredCanaryRef.Namespace,
			Name:      desiredCanaryRef.Name,
		}
		canaryObj := &flaggerv1.Canary{}
		err = kubeClient.Get(ctx, canaryKey, canaryObj)
		if errors.IsNotFound(err) {
			// TODO: override serivce and analysis config according to cluster label selector.
			canary := &flaggerv1.Canary{
				ObjectMeta: metav1.ObjectMeta{
					Name:      mcc.Name,
					Namespace: mcc.GetCanaryNamespace(),
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(mcc, schema.GroupVersionKind{
							Group:   multiclusterv1alpha1.GroupVersion.Group,
							Version: multiclusterv1alpha1.GroupVersion.Version,
							Kind:    "MultiClusterCanary",
						}),
					},
				},
				Spec: canarySpec,
			}
			if err = kubeClient.Create(ctx, canary); err != nil {
				return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
			}
			log.Info("created canary successfully", "cluster", clusterKey)

			for i, item := range mcc.Status.Inevntory {
				if item.ClusterName == desiredCanaryRef.ClusterName &&
					item.ClusterNamespace == desiredCanaryRef.ClusterNamespace &&
					item.Name == desiredCanaryRef.Name && item.Namespace == desiredCanaryRef.Namespace {
					mcc.Status.Inevntory = remove(mcc.Status.Inevntory, i)
				}
			}
			mcc.Status.Inevntory = append(mcc.Status.Inevntory, desiredCanaryRef)
			fmt.Println(mcc.Status.Inevntory)
			continue
		} else if err != nil {
			return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
		}

		if diff := cmp.Diff(canaryObj.Spec, canarySpec); diff != "" {
			log.Info("found diff in downstream canary", "cluster", clusterKey, "diff", diff)
			canaryObj.Spec = commonCanarySpec
			if err = kubeClient.Update(ctx, canaryObj); err != nil {
				return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
			}
			log.Info("updated canary successfully", "cluster", clusterKey)
		}
	}
	if err = r.Status().Update(ctx, mcc); err != nil {
		return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()},
			fmt.Errorf("failed to update status: %w", err)
	}

	return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MultiClusterCanaryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&multiclusterv1alpha1.MultiClusterCanary{}, builder.WithPredicates(
			predicate.GenerationChangedPredicate{},
		)).
		Complete(r)
}

func (r *MultiClusterCanaryReconciler) getClientForCluster(ctx context.Context,
	gitopsClusterObj *multiclusterv1alpha1.GitopsCluster) (client.Client, error) {
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: gitopsClusterObj.Namespace,
		Name:      gitopsClusterObj.Spec.SecretRef.Name,
	}
	if err := r.Get(ctx, secretKey, secret); err != nil {
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
		Scheme: r.Client.Scheme(),
		Mapper: restMapper,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to construct kube client: %w", err)
	}

	return kubeClient, err
}

func diff(a, b []multiclusterv1alpha1.CanaryObjRef) []multiclusterv1alpha1.CanaryObjRef {
	mb := make(map[multiclusterv1alpha1.CanaryObjRef]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []multiclusterv1alpha1.CanaryObjRef
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

func remove(s []multiclusterv1alpha1.CanaryObjRef, i int) []multiclusterv1alpha1.CanaryObjRef {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}
