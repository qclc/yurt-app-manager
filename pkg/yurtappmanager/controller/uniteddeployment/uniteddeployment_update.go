/*
Copyright 2021 The OpenYurt Authors.
Copyright 2019 The Kruise Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@CHANGELOG
OpenYurt Authors:
Subset to pool
*/

package uniteddeployment

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog"

	unitv1alpha1 "github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/apis/apps/v1alpha1"
	"github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/util"
)

// 调谐ud *unitv1alpha1.UnitedDeployment实例中的pools, 使得当前的pools资源符合期望的状态
func (r *ReconcileUnitedDeployment) managePools(ud *unitv1alpha1.UnitedDeployment,
	nameToPool map[string]*Pool, nextPatches map[string]UnitedDeploymentPatches,
	expectedRevision *appsv1.ControllerRevision,
	poolType unitv1alpha1.TemplateType) (newStatus *unitv1alpha1.UnitedDeploymentStatus, updateErr error) {

	newStatus = ud.Status.DeepCopy()
	// 将不是期望的pools删除, 创建目前没有的期望的pools, 返回的是(未调整前符合期望的已有的pool资源, 和是否经过调整, 调整过程中的错误)
	exists, provisioned, err := r.managePoolProvision(ud, nameToPool, nextPatches, expectedRevision, poolType)
	if err != nil {
		SetUnitedDeploymentCondition(newStatus, NewUnitedDeploymentCondition(unitv1alpha1.PoolProvisioned, corev1.ConditionFalse, "Error", err.Error()))
		return newStatus, fmt.Errorf("fail to manage Pool provision: %s", err)
	}

	if provisioned {
		SetUnitedDeploymentCondition(newStatus, NewUnitedDeploymentCondition(unitv1alpha1.PoolProvisioned, corev1.ConditionTrue, "", ""))
	}

	var needUpdate []string
	// 检查目前存在的符合期望状态的pool, 是否需要更新, 与nextPatches相比, 因为这一部分没有经过上面的创建或删除, 可能出现状态过时,
	// 需要的pool name加入needUpdate数组中
	for _, name := range exists.List() {
		pool := nameToPool[name]
		if r.poolControls[poolType].IsExpected(pool, expectedRevision.Name) ||
			pool.Status.ReplicasInfo.Replicas != nextPatches[name].Replicas ||
			pool.Status.PatchInfo != nextPatches[name].Patch {
			needUpdate = append(needUpdate, name)
		}
	}

	// 执行更新操作
	if len(needUpdate) > 0 {
		_, updateErr = util.SlowStartBatch(len(needUpdate), slowStartInitialBatchSize, func(index int) error {
			cell := needUpdate[index]
			pool := nameToPool[cell]
			replicas := nextPatches[cell].Replicas

			klog.Infof("UnitedDeployment %s/%s needs to update Pool (%s) %s/%s with revision %s, replicas %d ",
				ud.Namespace, ud.Name, poolType, pool.Namespace, pool.Name, expectedRevision.Name, replicas)

			// 在下面这个函数中, 会将ud中的patch
			updatePoolErr := r.poolControls[poolType].UpdatePool(pool, ud, expectedRevision.Name, replicas)
			if updatePoolErr != nil {
				r.recorder.Event(ud.DeepCopy(), corev1.EventTypeWarning, fmt.Sprintf("Failed%s", eventTypePoolsUpdate), fmt.Sprintf("Error updating PodSet (%s) %s when updating: %s", poolType, pool.Name, updatePoolErr))
			}
			return updatePoolErr
		})
	}

	if updateErr == nil {
		SetUnitedDeploymentCondition(newStatus, NewUnitedDeploymentCondition(unitv1alpha1.PoolUpdated, corev1.ConditionTrue, "", ""))
	} else {
		SetUnitedDeploymentCondition(newStatus, NewUnitedDeploymentCondition(unitv1alpha1.PoolUpdated, corev1.ConditionFalse, "Error", updateErr.Error()))
	}
	return
}

// 调整期望的pools和已有的pools(添加没有的, 删除多余的), 返回(未调整前符合期望的已有的pool资源, 和是否经过调整, 调整过程中的错误)
func (r *ReconcileUnitedDeployment) managePoolProvision(ud *unitv1alpha1.UnitedDeployment,
	nameToPool map[string]*Pool, nextPatches map[string]UnitedDeploymentPatches,
	expectedRevision *appsv1.ControllerRevision, workloadType unitv1alpha1.TemplateType) (sets.String, bool, error) {
	expectedPools := sets.String{}
	gotPools := sets.String{}

	// 从ud实例中获取期望的pools
	for _, pool := range ud.Spec.Topology.Pools {
		expectedPools.Insert(pool.Name)
	}

	//从nameToPool中提取现有的pools
	for poolName := range nameToPool {
		gotPools.Insert(poolName)
	}
	klog.V(4).Infof("UnitedDeployment %s/%s has pools %v, expects pools %v", ud.Namespace, ud.Name, gotPools.List(), expectedPools.List())

	// 将expectedPools中未在gotPools的pool加入待创造的creates列表中
	var creates []string
	for _, expectPool := range expectedPools.List() {
		if gotPools.Has(expectPool) {
			continue
		}

		creates = append(creates, expectPool)
	}

	// 将gotPools中未在expectedPools的pool加入待删除的deletes列表中
	var deletes []string
	for _, gotPool := range gotPools.List() {
		if expectedPools.Has(gotPool) {
			continue
		}

		deletes = append(deletes, gotPool)
	}

	revision := expectedRevision.Name

	var errs []error
	// manage creating
	if len(creates) > 0 {
		// do not consider deletion
		klog.Infof("UnitedDeployment %s/%s needs creating pool (%s) with name: %v", ud.Namespace, ud.Name, workloadType, creates)
		createdPools := make([]string, len(creates))
		for i, pool := range creates {
			createdPools[i] = pool
		}

		var createdNum int
		var createdErr error
		// 创建 creates 中的pool, 返回创建成功数, 和创建失败数
		createdNum, createdErr = util.SlowStartBatch(len(creates), slowStartInitialBatchSize, func(idx int) error {
			poolName := createdPools[idx]

			// 获取需要更新的patch信息中的Replicas数量
			replicas := nextPatches[poolName].Replicas
			// 创建pool
			err := r.poolControls[workloadType].CreatePool(ud, poolName, revision, replicas)
			if err != nil {
				if !errors.IsTimeout(err) {
					return fmt.Errorf("fail to create Pool (%s) %s: %s", workloadType, poolName, err.Error())
				}
			}

			return nil
		})
		if createdErr == nil {
			r.recorder.Eventf(ud.DeepCopy(), corev1.EventTypeNormal, fmt.Sprintf("Successful%s", eventTypePoolsUpdate), "Create %d Pool (%s)", createdNum, workloadType)
		} else {
			errs = append(errs, createdErr)
		}
	}

	// manage deleting
	if len(deletes) > 0 {
		klog.Infof("UnitedDeployment %s/%s needs deleting pool (%s) with name: [%v]", ud.Namespace, ud.Name, workloadType, deletes)
		var deleteErrs []error
		for _, poolName := range deletes {
			pool := nameToPool[poolName]
			if err := r.poolControls[workloadType].DeletePool(pool); err != nil {
				deleteErrs = append(deleteErrs, fmt.Errorf("fail to delete Pool (%s) %s/%s for %s: %s", workloadType, pool.Namespace, pool.Name, poolName, err))
			}
		}

		if len(deleteErrs) > 0 {
			errs = append(errs, deleteErrs...)
		} else {
			r.recorder.Eventf(ud.DeepCopy(), corev1.EventTypeNormal, fmt.Sprintf("Successful%s", eventTypePoolsUpdate), "Delete %d Pool (%s)", len(deletes), workloadType)
		}
	}

	// clean the other kind of pools
	// maybe user can chagne ud.Spec.WorkloadTemplate
	// 删除与workloadType不一样的其他类型的所有pool
	// 作用是, 用户可能修改了workloadType, 这样就要删除旧模板下的workload
	cleaned := false
	for t, control := range r.poolControls {
		if t == workloadType {
			continue
		}

		pools, err := control.GetAllPools(ud)
		if err != nil {
			errs = append(errs, fmt.Errorf("fail to list Pool of other type %s for UnitedDeployment %s/%s: %s", t, ud.Namespace, ud.Name, err))
			continue
		}

		for _, pool := range pools {
			cleaned = true
			if err := control.DeletePool(pool); err != nil {
				errs = append(errs, fmt.Errorf("fail to delete Pool %s of other type %s for UnitedDeployment %s/%s: %s", pool.Name, t, ud.Namespace, ud.Name, err))
				continue
			}
		}
	}

	// 返回 expectedPools 和 gotPools 的交集,
	return expectedPools.Intersection(gotPools), len(creates) > 0 || len(deletes) > 0 || cleaned, utilerrors.NewAggregate(errs)
}
