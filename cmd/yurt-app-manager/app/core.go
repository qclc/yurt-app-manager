/*
Copyright 2020 The OpenYurt Authors.

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

package app

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/openyurtio/yurt-app-manager/cmd/yurt-app-manager/options"
	"github.com/openyurtio/yurt-app-manager/pkg/projectinfo"
	appsv1alpha1 "github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/apis/apps/v1alpha1"
	extclient "github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/client"
	"github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/constant"
	"github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/controller"
	"github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/util/fieldindex"
	"github.com/openyurtio/yurt-app-manager/pkg/yurtappmanager/webhook"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog"
	"k8s.io/klog/klogr"
	ctrl "sigs.k8s.io/controller-runtime"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	//使用的令牌桶进行请求限流, 此处是设置controller处理请求的流量
	//Burst是桶里令牌数量初始值和上线, QPS相当于每秒往桶里发送的令牌数, 桶里超过Burst的令牌会被丢弃
	//每个请求需要一个令牌进行处理, 没有得到令牌的会阻塞直到令牌分配下来
	restConfigQPS   = flag.Int("rest-config-qps", 30, "QPS of rest config.")
	restConfigBurst = flag.Int("rest-config-burst", 50, "Burst of rest config.")
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = appsv1alpha1.AddToScheme(clientgoscheme.Scheme)

	_ = appsv1alpha1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

// NewCmdYurtAppManager creates a *cobra.Command object with default parameters
func NewCmdYurtAppManager(stopCh <-chan struct{}) *cobra.Command {
	yurtAppOptions := options.NewYurtAppOptions()

	cmd := &cobra.Command{
		Use:   projectinfo.GetYurtAppManagerName(),
		Short: "Launch " + projectinfo.GetYurtAppManagerName(),
		Long:  "Launch " + projectinfo.GetYurtAppManagerName(),
		Run: func(cmd *cobra.Command, args []string) {
			if yurtAppOptions.Version {
				fmt.Printf("%s: %#v\n", projectinfo.GetYurtAppManagerName(), projectinfo.Get())
				return
			}

			fmt.Printf("%s version: %#v\n", projectinfo.GetYurtAppManagerName(), projectinfo.Get())

			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				klog.V(1).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
			})
			if err := options.ValidateOptions(yurtAppOptions); err != nil {
				klog.Fatalf("validate options: %v", err)
			}

			Run(yurtAppOptions)
		},
	}

	yurtAppOptions.AddFlags(cmd.Flags())
	return cmd
}

func Run(opts *options.YurtAppOptions) {
	if opts.EnablePprof {
		go func() {
			if err := http.ListenAndServe(opts.PprofAddr, nil); err != nil {
				setupLog.Error(err, "unable to start pprof")
			}
		}()
	}

	//ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	ctrl.SetLogger(klogr.New())

	//通过kube-apiserver获取config
	cfg := ctrl.GetConfigOrDie()
	setRestConfig(cfg)

	//获取用于创建controller的manager
	//Manager 管理多个Controller 的运行，并提供 数据读（cache）写（client）等crudw基础能力
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                     scheme,
		MetricsBindAddress:         opts.MetricsAddr,
		HealthProbeBindAddress:     opts.HealthProbeAddr,
		LeaderElection:             opts.EnableLeaderElection,
		LeaderElectionID:           "yurt-app-manager",
		LeaderElectionNamespace:    opts.LeaderElectionNamespace,
		LeaderElectionResourceLock: resourcelock.LeasesResourceLock, // use lease to election
		Namespace:                  opts.Namespace,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	setupLog.Info("register field index")
	if err := fieldindex.RegisterFieldIndexes(mgr.GetCache()); err != nil {
		setupLog.Error(err, "failed to register field index")
		os.Exit(1)
	}

	setupLog.Info("new clientset registry")
	err = extclient.NewRegistry(mgr)
	if err != nil {
		setupLog.Error(err, "unable to init yurtapp clientset and informer")
		os.Exit(1)
	}

	setupLog.Info("setup controllers")

	//根据manager创建controller
	ctx := genOptCtx(opts.CreateDefaultPool)
	//将各个CRD的Reconciler传入controller进行, 用于单个资源的调谐作用
	if err = controller.SetupWithManager(mgr, ctx); err != nil {
		setupLog.Error(err, "unable to setup controllers")
		os.Exit(1)
	}

	//将webhook传入manager中, webhook用于准入验证的
	setupLog.Info("setup webhook")
	if err = webhook.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to setup webhook")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	stopCh := ctrl.SetupSignalHandler()
	setupLog.Info("initialize webhook")
	if err := webhook.Initialize(mgr, stopCh.Done()); err != nil {
		setupLog.Error(err, "unable to initialize webhook")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("webhook-ready", webhook.Checker); err != nil {
		setupLog.Error(err, "unable to add readyz check")
		os.Exit(1)
	}

	// 启动manager,即启动所有注册的Controller
	setupLog.Info("starting manager")
	if err := mgr.Start(stopCh); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}

}

func genOptCtx(createDefaultPool bool) context.Context {
	return context.WithValue(context.Background(),
		constant.ContextKeyCreateDefaultPool, createDefaultPool)
}

func setRestConfig(c *rest.Config) {
	if *restConfigQPS > 0 {
		c.QPS = float32(*restConfigQPS)
	}
	if *restConfigBurst > 0 {
		c.Burst = *restConfigBurst
	}
}
