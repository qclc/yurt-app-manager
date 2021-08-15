/*
Copyright 2020 The OpenYurt Authors.
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
change Adapter interface
*/

package adapter

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	alpha1 "github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/apis/apps/v1alpha1"
)

// 适配器, 在 appsv1.StatefulSet 和 appsv1.Deployment 之上抽象出来的一层, 提供相关资源的操作接口
type Adapter interface {
	// NewResourceObject creates a empty pool object.
	NewResourceObject() runtime.Object
	// NewResourceListObject creates a empty pool list object.
	// 返回空的pool list对象, 实际上就是&appsv1.StatefulSetList{}或&appsv1.DeploymentList{}
	NewResourceListObject() runtime.Object
	// GetStatusObservedGeneration returns the observed generation of the pool.
	GetStatusObservedGeneration(pool metav1.Object) int64
	// GetDetails returns the replicas information of the pool status.
	// 获取池子对象中的副本数目, 以及ready的副本数目,
	GetDetails(pool metav1.Object) (replicasInfo ReplicasInfo, err error)
	// GetPoolFailure returns failure information of the pool.
	GetPoolFailure() *string
	// ApplyPoolTemplate updates the pool to the latest revision.
	// 更新pool至最新的revision
	ApplyPoolTemplate(ud *alpha1.UnitedDeployment, poolName, revision string, replicas int32, pool runtime.Object) error
	// IsExpected checks the pool is the expected revision or not.
	// If not, UnitedDeployment will call ApplyPoolTemplate to update it.
	// 检查pool是否是期望的版本, 不是的话, UnitedDeployment会使用ApplyPoolTemplate来更新他
	IsExpected(pool metav1.Object, revision string) bool
	// PostUpdate does some works after pool updated
	PostUpdate(ud *alpha1.UnitedDeployment, pool runtime.Object, revision string) error
}

type ReplicasInfo struct {
	Replicas      int32
	ReadyReplicas int32
}
