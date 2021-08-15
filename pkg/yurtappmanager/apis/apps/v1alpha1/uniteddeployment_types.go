/*
Copyright 2021 The OpenYurt Authors.
Copyright 2020 The Kruise Authors.

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
change UnitedDeployment API Defination
*/

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type TemplateType string

const (
	StatefulSetTemplateType TemplateType = "StatefulSet"
	DeploymentTemplateType  TemplateType = "Deployment"
)

// UnitedDeploymentConditionType indicates valid conditions type of a UnitedDeployment.
type UnitedDeploymentConditionType string

const (
	// PoolProvisioned means all the expected pools are provisioned and unexpected pools are deleted.
	// PoolProvisioned 意味着预配所有预期的池并删除意外的池
	PoolProvisioned UnitedDeploymentConditionType = "PoolProvisioned"
	// PoolUpdated means all the pools are updated.
	// PoolUpdated代表所有的pools都已经更新
	PoolUpdated UnitedDeploymentConditionType = "PoolUpdated"
	// PoolFailure is added to a UnitedDeployment when one of its pools has failure during its own reconciling.
	// 当其中一个pool在调谐过程中出现错误时, 会将PoolFailure状态放到ud中
	PoolFailure UnitedDeploymentConditionType = "PoolFailure"
)

// UnitedDeploymentSpec defines the desired state of UnitedDeployment.
// UnitedDeploymentSpec 定义UnitedDeployment的期望状态
type UnitedDeploymentSpec struct {
	// Selector is a label query over pods that should match the replica count.
	// It must match the pod template's labels.
	// 选择器, 选择pod, 符合条件的pod数量应该符合replica数目
	Selector *metav1.LabelSelector `json:"selector"`

	// WorkloadTemplate describes the pool that will be created.
	// +optional
	// 表示要被创建的workload类型, DeploymentTemplate 或 StatefulSetTemplate
	WorkloadTemplate WorkloadTemplate `json:"workloadTemplate,omitempty"`

	// Topology describes the pods distribution detail between each of pools.
	// +optional
	// 描述pod在每个pools中的分布情况
	Topology Topology `json:"topology,omitempty"`

	// Indicates the number of histories to be conserved.
	// If unspecified, defaults to 10.
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty"`
}

// WorkloadTemplate defines the pool template under the UnitedDeployment.
// UnitedDeployment will provision every pool based on one workload templates in WorkloadTemplate.
// WorkloadTemplate now support statefulset and deployment
// Only one of its members may be specified.
type WorkloadTemplate struct {
	// StatefulSet template
	// +optional
	StatefulSetTemplate *StatefulSetTemplateSpec `json:"statefulSetTemplate,omitempty"`

	// Deployment template
	// +optional
	DeploymentTemplate *DeploymentTemplateSpec `json:"deploymentTemplate,omitempty"`
}

// StatefulSetTemplateSpec defines the pool template of StatefulSet.
type StatefulSetTemplateSpec struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              appsv1.StatefulSetSpec `json:"spec"`
}

// DeploymentTemplateSpec defines the pool template of Deployment.
type DeploymentTemplateSpec struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              appsv1.DeploymentSpec `json:"spec"`
}

// Topology defines the spread detail of each pool under UnitedDeployment.
// A UnitedDeployment manages multiple homogeneous workloads which are called pool.
// 一个 UnitedDeployment 管理多个同构工作负载, 称为pool
// Each of pools under the UnitedDeployment is described in Topology.
// UnitedDeployment 中的每个pool都包含了详细信息, 放在Topology
type Topology struct {
	// Contains the details of each pool. Each element in this array represents one pool
	// which will be provisioned and managed by UnitedDeployment.
	// +optional
	// 包含每个Pool的详细信息
	Pools []Pool `json:"pools,omitempty"`
}

// Pool defines the detail of a pool.
type Pool struct {
	// Indicates pool name as a DNS_LABEL, which will be used to generate
	// pool workload name prefix in the format '<deployment-name>-<pool-name>-'.
	// Name should be unique between all of the pools under one UnitedDeployment.
	// Name is NodePool Name
	// 遵循DNS命名格式, Pool Name 应该在同一个 UnitedDeployment 中保持唯一
	Name string `json:"name"`

	// Indicates the node selector to form the pool. Depending on the node selector,
	// pods provisioned could be distributed across multiple groups of nodes.
	// A pool's nodeSelectorTerm is not allowed to be updated.
	// +optional
	// 即这个pool中创建的pod可以被调度到哪些节点上
	// 该字段不允许进行更新
	NodeSelectorTerm corev1.NodeSelectorTerm `json:"nodeSelectorTerm,omitempty"`

	// Indicates the tolerations the pods under this pool have.
	// A pool's tolerations is not allowed to be updated.
	// +optional
	// 表示该pool中的pods 可以忍受哪些taints
	// 该字段不允许进行更新
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Indicates the number of the pod to be created under this pool.
	// 表示该pool中应该存在的pod数量
	// +required
	Replicas *int32 `json:"replicas,omitempty"`

	// Indicates the patch for the templateSpec
	// Now support strategic merge path :https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/#notes-on-the-strategic-merge-patch
	// Patch takes precedence over Replicas fields
	// If the Patch also modifies the Replicas, use the Replicas value in the Patch
	// +optional
	// 表示针对模板Spec的更新部分
	// Patch字段优先于Replicas字段。如果Patch也修改了Replicas，请使用Patch中的Replicas值
	Patch *runtime.RawExtension `json:"patch,omitempty"`
}

// UnitedDeploymentStatus defines the observed state of UnitedDeployment.
type UnitedDeploymentStatus struct {
	// ObservedGeneration is the most recent generation observed for this UnitedDeployment. It corresponds to the
	// UnitedDeployment's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Count of hash collisions for the UnitedDeployment. The UnitedDeployment controller
	// uses this field as a collision avoidance mechanism when it needs to
	// create the name for the newest ControllerRevision.
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty"`

	// CurrentRevision, if not empty, indicates the current version of the UnitedDeployment.
	CurrentRevision string `json:"currentRevision"`

	// Represents the latest available observations of a UnitedDeployment's current state.
	// +optional
	Conditions []UnitedDeploymentCondition `json:"conditions,omitempty"`

	// Records the topology detail information of the replicas of each pool.
	// +optional
	PoolReplicas map[string]int32 `json:"poolReplicas,omitempty"`

	// The number of ready replicas.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`

	// Replicas is the most recently observed number of replicas.
	Replicas int32 `json:"replicas"`

	// TemplateType indicates the type of PoolTemplate
	TemplateType TemplateType `json:"templateType"`
}

// UnitedDeploymentCondition describes current state of a UnitedDeployment.
type UnitedDeploymentCondition struct {
	// Type of in place set condition.
	// condition的种类
	Type UnitedDeploymentConditionType `json:"type,omitempty"`

	// Status of the condition, one of True, False, Unknown.
	// condition的状态, True, False, Unknown.
	Status corev1.ConditionStatus `json:"status,omitempty"`

	// Last time the condition transitioned from one status to another.
	// 上次condition转变的时间
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`

	// The reason for the condition's last transition.
	// condition最新一次转变的原因
	Reason string `json:"reason,omitempty"`

	// A human readable message indicating details about the transition.
	// 一条人类可读的消息，指示有关转换的详细信息。
	Message string `json:"message,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ud
// +kubebuilder:printcolumn:name="READY",type="integer",JSONPath=".status.readyReplicas",description="The number of pods ready."
// +kubebuilder:printcolumn:name="WorkloadTemplate",type="string",JSONPath=".status.templateType",description="The WorkloadTemplate Type."
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp",description="CreationTimestamp is a timestamp representing the server time when this object was created. It is not guaranteed to be set in happens-before order across separate operations. Clients may not set this value. It is represented in RFC3339 form and is in UTC."

// UnitedDeployment is the Schema for the uniteddeployments API
type UnitedDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   UnitedDeploymentSpec   `json:"spec,omitempty"`
	Status UnitedDeploymentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// UnitedDeploymentList contains a list of UnitedDeployment
type UnitedDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []UnitedDeployment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&UnitedDeployment{}, &UnitedDeploymentList{})
}
