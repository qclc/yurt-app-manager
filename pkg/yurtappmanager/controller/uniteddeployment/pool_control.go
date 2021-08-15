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
*/

package uniteddeployment

import (
	"context"
	"errors"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"

	alpha1 "github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/apis/apps/v1alpha1"
	"github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/controller/uniteddeployment/adapter"
	"github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/util/refmanager"
)

// PoolControl provides pool operations of MutableSet.
type PoolControl struct {
	client.Client

	scheme  *runtime.Scheme
	adapter adapter.Adapter
}

// GetAllPools returns all of pools owned by the UnitedDeployment.
// 获取所有属于UnitedDeployment的pool
// 1. 首先根据ud.Spec.Selector筛选出所有符合条件的Deployments或StatefulSet(List请求), 具体是哪一种根据workloadTemplate类型决定
// 2. 进行筛选,将获得的objs中owner是该UnitedDeployment的对象筛选出来
// 3. 将筛选出来的objs一个个转换为pool结构体, 最后组成pools返回
func (m *PoolControl) GetAllPools(ud *alpha1.UnitedDeployment) (pools []*Pool, err error) {
	selector, err := metav1.LabelSelectorAsSelector(ud.Spec.Selector)
	if err != nil {
		return nil, err
	}

	// 获取DeploymentList 或 StatefulSetList, 类型根据workloadTemplate确定, 一个ud实例中的所有pool类型相同.
	setList := m.adapter.NewResourceListObject()
	cliSetList, ok := setList.(client.ObjectList)
	if !ok {
		return nil, errors.New("fail to convert runtime object to client.ObjectList")
	}
	// 从kube-apiserver处获取符合筛选条件的pool资源, 即获取符合特定标签的Deployment 或 StatefulSet
	err = m.Client.List(context.TODO(), cliSetList, &client.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}

	// 获取管理pod的controllerRef的manager
	manager, err := refmanager.New(m.Client, ud.Spec.Selector, ud, m.scheme)
	if err != nil {
		return nil, err
	}

	// 提取List中的items字段, 将其中的对象一个个保存在selected中
	v := reflect.ValueOf(setList).Elem().FieldByName("Items")
	selected := make([]metav1.Object, v.Len())
	for i := 0; i < v.Len(); i++ {
		selected[i] = v.Index(i).Addr().Interface().(metav1.Object)
	}
	// 筛选对象, 将owner是该UnitedDeployment的对象存放到claimedSets中
	claimedSets, err := manager.ClaimOwnedObjects(selected)
	if err != nil {
		return nil, err
	}

	// 将claimedSets中的每一个Deployment或StatefulSet对象转换为Pool对象
	for _, claimedSet := range claimedSets {
		// 通过claimedSet的信息构建一个pool对象
		pool, err := m.convertToPool(claimedSet)
		if err != nil {
			return nil, err
		}
		pools = append(pools, pool)
	}
	return pools, nil
}

// CreatePool creates the Pool depending on the inputs.
func (m *PoolControl) CreatePool(ud *alpha1.UnitedDeployment, poolName string, revision string,
	replicas int32) error {

	set := m.adapter.NewResourceObject()
	m.adapter.ApplyPoolTemplate(ud, poolName, revision, replicas, set)

	klog.V(4).Infof("Have %d replicas when creating Pool for UnitedDeployment %s/%s", replicas, ud.Namespace, ud.Name)
	cliSet, ok := set.(client.Object)
	if !ok {
		return errors.New("fail to convert runtime.Object to client.Object")
	}
	return m.Create(context.TODO(), cliSet)
}

// UpdatePool is used to update the pool. The target Pool workload can be found with the input pool.
func (m *PoolControl) UpdatePool(pool *Pool, ud *alpha1.UnitedDeployment, revision string, replicas int32) error {
	set := m.adapter.NewResourceObject()
	cliSet, ok := set.(client.Object)
	if !ok {
		return errors.New("fail to convert runtime.Object to client.Object")
	}
	var updateError error
	for i := 0; i < updateRetries; i++ {
		// 根据pool的Namespace和Name获取kube-apiserver中的pool
		getError := m.Client.Get(context.TODO(), m.objectKey(pool), cliSet)
		if getError != nil {
			return getError
		}
		// 将传入的set更新为ud中的对应的pool设置, 同时如果ud中有相关的patch更新, 也会更新到set中
		// 此处为什么不是cliSet
		if err := m.adapter.ApplyPoolTemplate(ud, pool.Name, revision, replicas, set); err != nil {
			return err
		}
		// 更新至kube-apiserver上
		updateError = m.Client.Update(context.TODO(), cliSet)
		if updateError == nil {
			break
		}
	}

	if updateError != nil {
		return updateError
	}

	// 更新完成后进行的一些工作
	return m.adapter.PostUpdate(ud, set, revision)
}

// DeletePool is called to delete the pool. The target Pool workload can be found with the input pool.
func (m *PoolControl) DeletePool(pool *Pool) error {
	set := pool.Spec.PoolRef.(runtime.Object)
	cliSet, ok := set.(client.Object)
	if !ok {
		return errors.New("fail to convert runtime.Object to client.Object")
	}
	return m.Delete(context.TODO(), cliSet, client.PropagationPolicy(metav1.DeletePropagationBackground))
}

// GetPoolFailure return the error message extracted form Pool workload status conditions.
func (m *PoolControl) GetPoolFailure(pool *Pool) *string {
	return m.adapter.GetPoolFailure()
}

// IsExpected checks the pool is expected revision or not.
func (m *PoolControl) IsExpected(pool *Pool, revision string) bool {
	return m.adapter.IsExpected(pool.Spec.PoolRef, revision)
}

func (m *PoolControl) convertToPool(set metav1.Object) (*Pool, error) {
	// 通过对象的apps.openyurt.io/pool-name标签获取器所属那个池
	poolName, err := getPoolNameFrom(set)
	if err != nil {
		return nil, err
	}
	// 获取池子对象中的副本数目, 以及ready的副本数目, 都保存在specReplicas中
	specReplicas, err := m.adapter.GetDetails(set)
	if err != nil {
		return nil, err
	}
	pool := &Pool{
		Name:      poolName,
		Namespace: set.GetNamespace(),
		Spec: PoolSpec{
			PoolRef: set,
		},
		Status: PoolStatus{
			ObservedGeneration: m.adapter.GetStatusObservedGeneration(set),
			ReplicasInfo:       specReplicas,
		},
	}
	if data, ok := set.GetAnnotations()[alpha1.AnnotationPatchKey]; ok {
		pool.Status.PatchInfo = data
	}
	return pool, nil
}

// 返回Pool的Namespace和Name
func (m *PoolControl) objectKey(pool *Pool) client.ObjectKey {
	return types.NamespacedName{
		Namespace: pool.Namespace,
		Name:      pool.Spec.PoolRef.GetName(),
	}
}
