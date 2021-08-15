/*
Copyright 2020 The OpenYurt Authors.
Copyright 2019 The Kruise Authors.
Copyright 2017 The Kubernetes Authors.

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

package uniteddeployment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller/history"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	appsalphav1 "github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/apis/apps/v1alpha1"
	"github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/util/refmanager"
)

// ControllerRevisionHashLabel is the label used to indicate the hash value of a ControllerRevision's Data.
const ControllerRevisionHashLabel = "controller.kubernetes.io/hash"

// 获取符合ud.Spec.Selector条件的, 且owner处于ud的histories
func (r *ReconcileUnitedDeployment) controlledHistories(ud *appsalphav1.UnitedDeployment) ([]*apps.ControllerRevision, error) {
	// List all histories to include those that don't match the selector anymore
	// but have a ControllerRef pointing to the controller.
	selector, err := metav1.LabelSelectorAsSelector(ud.Spec.Selector)
	if err != nil {
		return nil, err
	}
	// ControllerRevision资源保存了不同历史版本的创建该对象所使用的模板(可理解为yaml文件), 用于历史回滚用
	histories := &apps.ControllerRevisionList{}
	// 获取所有符合UnitedDeployment对象中selector条件的ControllerRevision资源对象
	err = r.Client.List(context.TODO(), histories, &client.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	klog.V(1).Infof("List controller revision of UnitedDeployment %s/%s: count %d\n", ud.Namespace, ud.Name, len(histories.Items))

	// Use ControllerRefManager to adopt/orphan as needed.
	// 创建自定义的用于管理Pod的ControllerRevision的对象,会暴露一些方法
	cm, err := refmanager.New(r.Client, ud.Spec.Selector, ud, r.scheme)
	if err != nil {
		return nil, err
	}

	mts := make([]metav1.Object, len(histories.Items))
	for i, history := range histories.Items {
		mts[i] = history.DeepCopy()
	}
	// 找到histories中owner属于该ud的
	claims, err := cm.ClaimOwnedObjects(mts)
	if err != nil {
		return nil, err
	}

	claimHistories := make([]*apps.ControllerRevision, len(claims))
	for i, mt := range claims {
		claimHistories[i] = mt.(*apps.ControllerRevision)
	}

	return claimHistories, nil
}

// 构建UnitedDeployment对象的版本号Revisions
// 返回(当前status中的版本号, 更新后的版本号, 更新后的CollisionCount, 错误)
func (r *ReconcileUnitedDeployment) constructUnitedDeploymentRevisions(ud *appsalphav1.UnitedDeployment) (*apps.ControllerRevision, *apps.ControllerRevision, int32, error) {
	var currentRevision, updateRevision *apps.ControllerRevision
	// 获取符合ud.Spec.Selector条件的, 且owner属于ud的histories
	revisions, err := r.controlledHistories(ud)
	if err != nil {
		if ud.Status.CollisionCount == nil {
			return currentRevision, updateRevision, 0, err
		}
		return currentRevision, updateRevision, *ud.Status.CollisionCount, err
	}

	//根据revisions对ControllerRevision对象进行排序, 先小后大, 比较先后为: 1.revision, 2.creation timestamp, 3.name
	history.SortControllerRevisions(revisions)
	// 获取清理过后的历史版本列表
	cleanedRevision, err := r.cleanExpiredRevision(ud, &revisions)
	if err != nil {
		if ud.Status.CollisionCount == nil {
			return currentRevision, updateRevision, 0, err
		}
		return currentRevision, updateRevision, *ud.Status.CollisionCount, err
	}
	revisions = *cleanedRevision

	// Use a local copy of set.Status.CollisionCount to avoid modifying set.Status directly.
	// This copy is returned so the value gets carried over to set.Status in updateStatefulSet.
	// 使用 set.Status.CollisionCount 的本地副本以避免直接修改 set.Status。
	// 返回此副本，以便将值传递到 updateStatefulSet 中的 set.Status。
	var collisionCount int32
	if ud.Status.CollisionCount != nil {
		collisionCount = *ud.Status.CollisionCount
	}

	// create a new revision from the current set
	// 根据当前的set创建一个新的版本
	updateRevision, err = r.newRevision(ud, nextRevision(revisions), &collisionCount)
	if err != nil {
		return nil, nil, collisionCount, err
	}

	// find any equivalent revisions
	// 返回历史版本中跟当前版本相同的版本列表
	equalRevisions := history.FindEqualRevisions(revisions, updateRevision)
	equalCount := len(equalRevisions)
	revisionCount := len(revisions)

	if equalCount > 0 && history.EqualRevision(revisions[revisionCount-1], equalRevisions[equalCount-1]) {
		// if the equivalent revision is immediately prior the update revision has not changed
		// 如果等效修订版紧邻更新修订版, 则不做更改, 直接使用旧的等效版本
		updateRevision = revisions[revisionCount-1]
	} else if equalCount > 0 {
		// if the equivalent revision is not immediately prior we will roll back by incrementing the
		// Revision of the equivalent revision
		// 如果等效修订不是紧挨着的，我们将通过增加最新的修订来达到回滚效果
		equalRevisions[equalCount-1].Revision = updateRevision.Revision
		err := r.Client.Update(context.TODO(), equalRevisions[equalCount-1])
		if err != nil {
			return nil, nil, collisionCount, err
		}
		updateRevision = equalRevisions[equalCount-1]
	} else {
		//if there is no equivalent revision we create a new one
		// 如果没有等效的, 则创建一个全新的
		updateRevision, err = r.createControllerRevision(ud, updateRevision, &collisionCount)
		if err != nil {
			return nil, nil, collisionCount, err
		}
	}

	// attempt to find the revision that corresponds to the current revision
	for i := range revisions {
		if revisions[i].Name == ud.Status.CurrentRevision {
			currentRevision = revisions[i]
		}
	}

	// if the current revision is nil we initialize the history by setting it to the update revision
	if currentRevision == nil {
		currentRevision = updateRevision
	}

	return currentRevision, updateRevision, collisionCount, nil
}

// 清除超出保存数目的, 过期的版本, 并返回清理过后的版本
func (r *ReconcileUnitedDeployment) cleanExpiredRevision(ud *appsalphav1.UnitedDeployment,
	sortedRevisions *[]*apps.ControllerRevision) (*[]*apps.ControllerRevision, error) {

	exceedNum := len(*sortedRevisions) - int(*ud.Spec.RevisionHistoryLimit)
	if exceedNum <= 0 {
		return sortedRevisions, nil
	}

	live := map[string]bool{}
	live[ud.Status.CurrentRevision] = true

	for i, revision := range *sortedRevisions {
		if _, exist := live[revision.Name]; exist {
			continue
		}

		if i >= exceedNum {
			break
		}
		// 将超出的保留数量的, 过期的从kube-apiserver中删除
		if err := r.Client.Delete(context.TODO(), revision); err != nil {
			return sortedRevisions, err
		}
	}
	cleanedRevisions := (*sortedRevisions)[exceedNum:]

	return &cleanedRevisions, nil
}

// createControllerRevision creates the controller revision owned by the parent.
func (r *ReconcileUnitedDeployment) createControllerRevision(parent metav1.Object, revision *apps.ControllerRevision, collisionCount *int32) (*apps.ControllerRevision, error) {
	if collisionCount == nil {
		return nil, fmt.Errorf("collisionCount should not be nil")
	}

	// Clone the input
	clone := revision.DeepCopy()

	var err error
	// Continue to attempt to create the revision updating the name with a new hash on each iteration
	for {
		hash := history.HashControllerRevision(revision, collisionCount)
		// Update the revisions name
		clone.Name = history.ControllerRevisionName(parent.GetName(), hash)
		err = r.Client.Create(context.TODO(), clone)
		if errors.IsAlreadyExists(err) {
			exists := &apps.ControllerRevision{}
			err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: parent.GetNamespace(), Name: clone.Name}, exists)
			if err != nil {
				return nil, err
			}
			if bytes.Equal(exists.Data.Raw, clone.Data.Raw) {
				return exists, nil
			}
			*collisionCount++
			continue
		}
		return clone, err
	}
}

// newRevision creates a new ControllerRevision containing a patch that reapplies the target state of set.
// The Revision of the returned ControllerRevision is set to revision. If the returned error is nil, the returned
// ControllerRevision is valid. StatefulSet revisions are stored as patches that re-apply the current state of set
// to a new StatefulSet using a strategic merge patch to replace the saved state of the new StatefulSet.
func (r *ReconcileUnitedDeployment) newRevision(ud *appsalphav1.UnitedDeployment, revision int64, collisionCount *int32) (*apps.ControllerRevision, error) {
	patch, err := getUnitedDeploymentPatch(ud)
	if err != nil {
		return nil, err
	}

	gvk, err := apiutil.GVKForObject(ud, r.scheme)
	if err != nil {
		return nil, err
	}

	var selectedLabels map[string]string
	switch {
	case ud.Spec.WorkloadTemplate.StatefulSetTemplate != nil:
		selectedLabels = ud.Spec.WorkloadTemplate.StatefulSetTemplate.Labels
	case ud.Spec.WorkloadTemplate.DeploymentTemplate != nil:
		selectedLabels = ud.Spec.WorkloadTemplate.DeploymentTemplate.Labels
	default:
		klog.Errorf("UnitedDeployment(%s/%s) need specific WorkloadTemplate", ud.GetNamespace(), ud.GetName())
		return nil, fmt.Errorf("UnitedDeployment(%s/%s) need specific WorkloadTemplate", ud.GetNamespace(), ud.GetName())
	}

	cr, err := history.NewControllerRevision(ud,
		gvk,
		selectedLabels,
		runtime.RawExtension{Raw: patch},
		revision,
		collisionCount)
	if err != nil {
		return nil, err
	}
	cr.Namespace = ud.Namespace

	return cr, nil
}

// nextRevision finds the next valid revision number based on revisions. If the length of revisions
// is 0 this is 1. Otherwise, it is 1 greater than the largest revision's Revision. This method
// assumes that revisions has been sorted by Revision.
// 基于现有的版本号返回下一个有效的revision版本号
func nextRevision(revisions []*apps.ControllerRevision) int64 {
	count := len(revisions)
	if count <= 0 {
		return 1
	}
	return revisions[count-1].Revision + 1
}

func getUnitedDeploymentPatch(ud *appsalphav1.UnitedDeployment) ([]byte, error) {
	dsBytes, err := json.Marshal(ud)
	if err != nil {
		return nil, err
	}
	var raw map[string]interface{}
	err = json.Unmarshal(dsBytes, &raw)
	if err != nil {
		return nil, err
	}
	objCopy := make(map[string]interface{})
	specCopy := make(map[string]interface{})

	// Create a patch of the UnitedDeployment that replaces spec.template
	spec := raw["spec"].(map[string]interface{})
	template := spec["workloadTemplate"].(map[string]interface{})
	specCopy["workloadTemplate"] = template
	template["$patch"] = "replace"
	objCopy["spec"] = specCopy
	patch, err := json.Marshal(objCopy)
	return patch, err
}
