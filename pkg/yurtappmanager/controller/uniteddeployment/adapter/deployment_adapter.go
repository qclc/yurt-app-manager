/*
Copyright 2021 The OpenYurt Authors.

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

package adapter

import (
	"fmt"

	"k8s.io/klog"

	alpha1 "github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/apis/apps/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type DeploymentAdapter struct {
	client.Client

	Scheme *runtime.Scheme
}

var _ Adapter = &DeploymentAdapter{}

// NewResourceObject creates a empty Deployment object.
func (a *DeploymentAdapter) NewResourceObject() runtime.Object {
	return &appsv1.Deployment{}
}

// NewResourceListObject creates a empty DeploymentList object.
func (a *DeploymentAdapter) NewResourceListObject() runtime.Object {
	return &appsv1.DeploymentList{}
}

// GetStatusObservedGeneration returns the observed generation of the pool.
func (a *DeploymentAdapter) GetStatusObservedGeneration(obj metav1.Object) int64 {
	return obj.(*appsv1.Deployment).Status.ObservedGeneration
}

// GetDetails returns the replicas detail the pool needs.
func (a *DeploymentAdapter) GetDetails(obj metav1.Object) (ReplicasInfo, error) {
	set := obj.(*appsv1.Deployment)

	var specReplicas int32
	if set.Spec.Replicas != nil {
		specReplicas = *set.Spec.Replicas
	}
	replicasInfo := ReplicasInfo{
		Replicas:      specReplicas,
		ReadyReplicas: set.Status.ReadyReplicas,
	}
	return replicasInfo, nil
}

// GetPoolFailure returns the failure information of the pool.
// Deployment has no condition.
func (a *DeploymentAdapter) GetPoolFailure() *string {
	return nil
}

// ApplyPoolTemplate updates the pool to the latest revision, depending on the DeploymentTemplate.
// 将传入的obj更新为ud中的对应的pool设置, 同时如果ud中有相关的patch更新, 也会更新到obj中
func (a *DeploymentAdapter) ApplyPoolTemplate(ud *alpha1.UnitedDeployment, poolName, revision string,
	replicas int32, obj runtime.Object) error {
	// set是传入的obj
	set := obj.(*appsv1.Deployment)

	// 从ud中找到名字为poolName的pool存入poolConfig
	var poolConfig *alpha1.Pool
	for i, pool := range ud.Spec.Topology.Pools {
		if pool.Name == poolName {
			poolConfig = &(ud.Spec.Topology.Pools[i])
			break
		}
	}
	if poolConfig == nil {
		return fmt.Errorf("fail to find pool config %s", poolName)
	}

	// 设置传入obj中的label, Namespace, Annotations与 ud 保持一致
	set.Namespace = ud.Namespace

	if set.Labels == nil {
		set.Labels = map[string]string{}
	}
	for k, v := range ud.Spec.WorkloadTemplate.DeploymentTemplate.Labels {
		set.Labels[k] = v
	}
	for k, v := range ud.Spec.Selector.MatchLabels {
		set.Labels[k] = v
	}
	set.Labels[alpha1.ControllerRevisionHashLabelKey] = revision
	// record the pool name as a label
	set.Labels[alpha1.PoolNameLabelKey] = poolName

	if set.Annotations == nil {
		set.Annotations = map[string]string{}
	}
	for k, v := range ud.Spec.WorkloadTemplate.DeploymentTemplate.Annotations {
		set.Annotations[k] = v
	}

	set.GenerateName = getPoolPrefix(ud.Name, poolName)

	selectors := ud.Spec.Selector.DeepCopy()
	selectors.MatchLabels[alpha1.PoolNameLabelKey] = poolName

	if err := controllerutil.SetControllerReference(ud, set, a.Scheme); err != nil {
		return err
	}

	set.Spec.Selector = selectors
	set.Spec.Replicas = &replicas

	set.Spec.Strategy = *ud.Spec.WorkloadTemplate.DeploymentTemplate.Spec.Strategy.DeepCopy()
	set.Spec.Template = *ud.Spec.WorkloadTemplate.DeploymentTemplate.Spec.Template.DeepCopy()
	if set.Spec.Template.Labels == nil {
		set.Spec.Template.Labels = map[string]string{}
	}
	set.Spec.Template.Labels[alpha1.PoolNameLabelKey] = poolName
	set.Spec.Template.Labels[alpha1.ControllerRevisionHashLabelKey] = revision

	set.Spec.RevisionHistoryLimit = ud.Spec.RevisionHistoryLimit
	set.Spec.MinReadySeconds = ud.Spec.WorkloadTemplate.DeploymentTemplate.Spec.MinReadySeconds
	set.Spec.Paused = ud.Spec.WorkloadTemplate.DeploymentTemplate.Spec.Paused
	set.Spec.ProgressDeadlineSeconds = ud.Spec.WorkloadTemplate.DeploymentTemplate.Spec.ProgressDeadlineSeconds

	attachNodeAffinityAndTolerations(&set.Spec.Template.Spec, poolConfig)

	// 判断ud中保存的对应pool是否存在patch
	if !PoolHasPatch(poolConfig, set) {
		klog.Infof("Deployment[%s/%s-] has no patches, do not need strategicmerge", set.Namespace,
			set.GenerateName)
		return nil
	}

	// 根据patch创建一个新的Deployment
	patched := &appsv1.Deployment{}
	if err := CreateNewPatchedObject(poolConfig.Patch, set, patched); err != nil {
		klog.Errorf("Deployment[%s/%s-] strategic merge by patch %s error %v", set.Namespace,
			set.GenerateName, string(poolConfig.Patch.Raw), err)
		return err
	}
	// 将patched写入传入的obj中
	patched.DeepCopyInto(set)

	klog.Infof("Deployment [%s/%s-] has patches configure successfully:%v", set.Namespace,
		set.GenerateName, string(poolConfig.Patch.Raw))
	return nil
}

// PostUpdate does some works after pool updated. Deployment will implement this method to clean stuck pods.
// PostUpdate 用于在池pool更新后做一些额外工作。Deployment将应用此方法来清理卡住的 pod。
func (a *DeploymentAdapter) PostUpdate(ud *alpha1.UnitedDeployment, obj runtime.Object, revision string) error {
	// Do nothing,
	return nil
}

// IsExpected checks the pool is the expected revision or not.
// The revision label can tell the current pool revision.
func (a *DeploymentAdapter) IsExpected(obj metav1.Object, revision string) bool {
	return obj.GetLabels()[alpha1.ControllerRevisionHashLabelKey] != revision
}
