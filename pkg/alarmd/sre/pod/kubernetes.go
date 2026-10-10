// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package pod

import (
	"context"
	"errors"
	"io"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/client-go/util/exec"
)

type KubernetesClient struct {
	core   typedcore.CoreV1Interface
	owners dynamic.Interface
}

// NewInCluster constructs the production adapters from the ServiceAccount.
// Availability errors can be reported by the channel without blocking its auth
// lifecycle. No Pod, logs or execution requests are made during construction.
func NewInCluster(options Options) (*Provider, error) {
	if err := ValidateScopes(options.Scopes); err != nil {
		return nil, err
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, &Error{Code: "service_account_not_mounted", Message: "in-cluster configuration is unavailable"}
	}
	client, err := NewKubernetesClient(config)
	if err != nil {
		return nil, err
	}
	executor, err := NewKubernetesExecutor(config)
	if err != nil {
		return nil, err
	}
	options.Client = client
	options.Executor = executor
	return New(options)
}

// NewKubernetesClient uses the server's ServiceAccount when config is nil.
// Callers may share a rest.InClusterConfig result with the exec constructor.
func NewKubernetesClient(config *rest.Config) (*KubernetesClient, error) {
	if config == nil {
		var err error
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, &Error{Code: "service_account_not_mounted", Message: "in-cluster configuration is unavailable"}
		}
	}
	core, err := typedcore.NewForConfig(rest.CopyConfig(config))
	if err != nil {
		return nil, apiError(err)
	}
	owners, err := dynamic.NewForConfig(rest.CopyConfig(config))
	if err != nil {
		return nil, apiError(err)
	}
	return &KubernetesClient{core: core, owners: owners}, nil
}
func (c *KubernetesClient) ListPods(ctx context.Context, namespace string, options metav1.ListOptions) (*corev1.PodList, error) {
	return c.core.Pods(namespace).List(ctx, options)
}
func (c *KubernetesClient) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	return c.core.Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}
func (c *KubernetesClient) OpenLogs(ctx context.Context, namespace, name string, options corev1.PodLogOptions) (io.ReadCloser, error) {
	return c.core.Pods(namespace).GetLogs(name, &options).Stream(ctx)
}

var ownerResources = map[string]schema.GroupVersionResource{
	"ReplicaSet":  {Group: "apps", Version: "v1", Resource: "replicasets"},
	"Deployment":  {Group: "apps", Version: "v1", Resource: "deployments"},
	"StatefulSet": {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"DaemonSet":   {Group: "apps", Version: "v1", Resource: "daemonsets"},
	"Job":         {Group: "batch", Version: "v1", Resource: "jobs"},
	"CronJob":     {Group: "batch", Version: "v1", Resource: "cronjobs"},
}

func (c *KubernetesClient) GetOwner(ctx context.Context, namespace string, ref metav1.OwnerReference) (OwnerResource, error) {
	resource, ok := ownerResources[ref.Kind]
	if !ok {
		return OwnerResource{}, denied("unsupported owner resource")
	}
	if ref.APIVersion != resource.Group+"/"+resource.Version {
		return OwnerResource{}, denied("unsupported owner API version")
	}
	obj, err := c.owners.Resource(resource).Namespace(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return OwnerResource{}, err
	}
	result := OwnerResource{Kind: ref.Kind, Namespace: obj.GetNamespace(), Name: obj.GetName(), UID: string(obj.GetUID()), Labels: obj.GetLabels(), Owners: obj.GetOwnerReferences()}
	selector, found, err := unstructured.NestedMap(obj.Object, "spec", "selector")
	if err != nil {
		return OwnerResource{}, err
	}
	if found {
		result.Selector = &metav1.LabelSelector{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(selector, result.Selector); err != nil {
			return OwnerResource{}, err
		}
	}
	return result, nil
}

type KubernetesExecutor struct {
	config     *rest.Config
	core       typedcore.CoreV1Interface
	parameters runtime.ParameterCodec
}

func NewKubernetesExecutor(config *rest.Config) (*KubernetesExecutor, error) {
	if config == nil {
		var err error
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, &Error{Code: "service_account_not_mounted", Message: "in-cluster configuration is unavailable"}
		}
	}
	copyConfig := rest.CopyConfig(config)
	core, err := typedcore.NewForConfig(copyConfig)
	if err != nil {
		return nil, apiError(err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, apiError(err)
	}
	return &KubernetesExecutor{config: copyConfig, core: core, parameters: runtime.NewParameterCodec(scheme)}, nil
}

// Stream opens exactly one standard Kubernetes SPDY exec connection. There is
// no fallback or reconnect that could execute the submitted command twice.
func (e *KubernetesExecutor) Stream(ctx context.Context, request StreamRequest) (StreamResult, error) {
	url := e.core.RESTClient().Post().Resource("pods").Namespace(request.Target.Namespace).Name(request.Target.Pod).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: request.Target.Container, Command: request.Argv, Stdin: request.Stdin != nil, Stdout: true, Stderr: true, TTY: false}, e.parameters).URL()
	executor, err := remotecommand.NewSPDYExecutor(e.config, http.MethodPost, url)
	if err != nil {
		return StreamResult{}, err
	}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: request.Stdin, Stdout: request.Stdout, Stderr: request.Stderr, Tty: false})
	if err == nil {
		code := 0
		return StreamResult{ExitCode: &code}, nil
	}
	var exit utilexec.ExitError
	if errors.As(err, &exit) {
		code := exit.ExitStatus()
		return StreamResult{ExitCode: &code}, nil
	}
	return StreamResult{}, err
}
