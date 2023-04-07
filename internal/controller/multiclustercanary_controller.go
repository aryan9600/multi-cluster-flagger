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
		log.Error(err, "")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	clusterSelector, err := metav1.LabelSelectorAsSelector(&mcc.Spec.GitopsClusterSelector)
	if err != nil {
		log.Error(err, "")
		return ctrl.Result{Requeue: true}, err
	}
	gitopsClusters := &multiclusterv1alpha1.GitopsClusterList{}
	if err := r.List(ctx, gitopsClusters, client.MatchingLabelsSelector{Selector: clusterSelector}); err != nil {
		log.Error(err, "")
		return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
	}

	var clusterKeys []string
	for _, cluster := range gitopsClusters.Items {
		clusterKeys = append(clusterKeys, fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name))
	}

	if len(mcc.Status.Clusters) > 0 {
		staleClusters := diff(mcc.Status.Clusters, clusterKeys)
		if len(staleClusters) > 0 {
			log.Info("found stale clusters", "keys", staleClusters)
		}
		for _, staleCluster := range staleClusters {
			staleClusterKey := getNamespacedNameFromString(staleCluster)
			staleClusterObj := &multiclusterv1alpha1.GitopsCluster{}
			if err := r.Get(ctx, staleClusterKey, staleClusterObj); err == nil {
				kubeClient, err := r.getKubeClient(ctx, staleClusterObj)
				if err != nil {
					return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
				}
				staleCanary := &flaggerv1.Canary{
					ObjectMeta: metav1.ObjectMeta{
						Name:      mcc.Name,
						Namespace: mcc.Spec.CanaryNamespace,
					},
				}
				if err = kubeClient.Delete(ctx, staleCanary, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
					log.Error(err, "")
					return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
				}
				log.Info("deleted stale cluster successfully", "cluster", staleCluster)
			}
		}
	}

	mcc.Status.Clusters = clusterKeys
	if err = r.Status().Update(ctx, mcc); err != nil {
		return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()},
			fmt.Errorf("failed to update status: %w", err)
	}

	commonCanarySpec := flaggerv1.CanarySpec{
		TargetRef:     mcc.Spec.TargetRef,
		AutoscalerRef: mcc.Spec.AutoscalerRef,
		UpstreamRef:   mcc.Spec.UpstreamRef,
		IngressRef:    mcc.Spec.IngressRef,
		RouteRef:      mcc.Spec.RouteRef,
		Service:       mcc.Spec.Service.CanaryService,
		Analysis:      &mcc.Spec.Analysis.CanaryAnalysis,
	}

	for _, clusterKey := range clusterKeys {
		cluster := &multiclusterv1alpha1.GitopsCluster{}
		if err := r.Client.Get(ctx, getNamespacedNameFromString(clusterKey), cluster); err != nil {
			return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
		}
		kubeClient, err := r.getKubeClient(ctx, cluster)
		if err != nil {
			return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
		}

		canaryKey := types.NamespacedName{
			Namespace: mcc.Spec.CanaryNamespace,
			Name:      mcc.Name,
		}
		existingCanary := &flaggerv1.Canary{}
		err = kubeClient.Get(ctx, canaryKey, existingCanary)
		if errors.IsNotFound(err) {
			// TODO: override serivce and analysis config according to cluster label selector.
			canary := &flaggerv1.Canary{
				ObjectMeta: metav1.ObjectMeta{
					Name:      mcc.Name,
					Namespace: mcc.Spec.CanaryNamespace,
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(mcc, schema.GroupVersionKind{
							Group:   multiclusterv1alpha1.GroupVersion.Group,
							Version: multiclusterv1alpha1.GroupVersion.Version,
							Kind:    "MultiClusterCanary",
						}),
					},
				},
				Spec: commonCanarySpec,
			}
			if err = kubeClient.Create(ctx, canary); err != nil {
				return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
			}
			log.Info("created canary successfully", "cluster", clusterKey)
			continue
		} else if err != nil {
			return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
		}

		if diff := cmp.Diff(existingCanary.Spec, commonCanarySpec); diff != "" {
			log.Info("found diff in downstream canary", "cluster", clusterKey, "diff", diff)
			existingCanary.Spec = commonCanarySpec
			if err = kubeClient.Update(ctx, existingCanary); err != nil {
				return ctrl.Result{RequeueAfter: mcc.GetRequeueAfter()}, err
			}
			log.Info("updated canary successfully", "cluster", clusterKey)
		}
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

func getNamespacedNameFromString(s string) types.NamespacedName {
	return types.NamespacedName{
		Namespace: strings.Split(s, "/")[0],
		Name:      strings.Split(s, "/")[1],
	}
}

func diff(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

func (r *MultiClusterCanaryReconciler) getKubeClient(ctx context.Context, gitopsClusterObj *multiclusterv1alpha1.GitopsCluster) (client.Client, error) {
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
