// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package steps

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/kustomize/api/konfig"
	"sigs.k8s.io/kustomize/api/types"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kustomize"
)

const (
	pprofComponentDir           = installDir + "/components/pprof"
	pprofContentionComponentDir = installDir + "/components/pprof-contention"
)

// composeDecoded renders target with components via kustomize.Compose and
// decodes the resulting Kubernetes manifests.
func composeDecoded(t *testing.T, target string, components ...string) []*unstructured.Unstructured {
	t.Helper()
	raw, err := kustomize.Compose(target, components...)
	if err != nil {
		t.Fatalf("kustomize.Compose(%s): %v", target, err)
	}
	objs, err := kube.DecodeManifestBytes(raw)
	if err != nil {
		t.Fatalf("decoding manifests for %s: %v", target, err)
	}
	return objs
}

// loadPprofSystemInstall renders all ate-system manifests for cfg with any
// extra component paths and returns the parsed NetworkPolicy/pod model plus
// each workload's containers keyed by Kind/Name.
func loadPprofSystemInstall(t *testing.T, cfg config.Config, extraRel ...string) (systemInstall, map[string][]corev1.Container) {
	t.Helper()
	root := repoRoot(t)
	cfg.Root = root
	e := &Env{Cfg: &cfg}

	components := e.components()
	for _, rel := range extraRel {
		components = append(components, filepath.Join(root, rel))
	}

	egress := e.atenetEgressManifestPath()
	if cfg.Router == config.RouterAgentgateway {
		egress = filepath.Join(root, installDir, "agentgateway-egress")
	}

	targets := []string{
		filepath.Join(root, SystemOverlay(&cfg)),
		e.postgresManifestPath(),
		egress,
		e.k8sCredentialProviderPath(k8sCredentialProviderManifest),
	}

	install := systemInstall{serviceAccounts: map[string]corev1.ServiceAccount{}}
	workloads := map[string][]corev1.Container{}
	for _, target := range targets {
		for _, obj := range composeDecoded(t, target, components...) {
			if obj.GetNamespace() != NamespaceAteSystem {
				continue
			}
			switch obj.GetKind() {
			case "NetworkPolicy":
				var np networkingv1.NetworkPolicy
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &np); err != nil {
					t.Fatalf("converting networkpolicy/%s: %v", obj.GetName(), err)
				}
				install.policies = append(install.policies, np)
			case "Deployment", "DaemonSet", "StatefulSet", "Job":
				raw, ok, _ := unstructured.NestedMap(obj.Object, "spec", "template")
				if !ok {
					t.Fatalf("%s/%s has no pod template", obj.GetKind(), obj.GetName())
				}
				var tmpl corev1.PodTemplateSpec
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &tmpl); err != nil {
					t.Fatalf("converting pod template for %s/%s: %v", obj.GetKind(), obj.GetName(), err)
				}
				workload := obj.GetKind() + "/" + obj.GetName()
				workloads[workload] = tmpl.Spec.Containers
				pod := systemPod{
					workload: workload,
					labels:   tmpl.Labels,
					spec:     tmpl.Spec,
				}
				if obj.GetKind() == "Job" {
					// The Job controller adds this label to its pods.
					pod.labels = map[string]string{"batch.kubernetes.io/job-name": obj.GetName()}
					for k, v := range tmpl.Labels {
						pod.labels[k] = v
					}
				}
				for _, cn := range slices.Concat(tmpl.Spec.InitContainers, tmpl.Spec.Containers) {
					pod.ports = append(pod.ports, cn.Ports...)
				}
				install.pods = append(install.pods, pod)
			}
		}
	}
	if len(install.pods) == 0 {
		t.Fatal("found no ate-system workloads after composing install")
	}
	return install, workloads
}

func findPolicy(t *testing.T, policies []networkingv1.NetworkPolicy, name string) networkingv1.NetworkPolicy {
	t.Helper()
	idx := slices.IndexFunc(policies, func(np networkingv1.NetworkPolicy) bool {
		return np.Name == name
	})
	if idx < 0 {
		t.Fatalf("NetworkPolicy/%s not found in ate-system install", name)
	}
	return policies[idx]
}

func findContainer(t *testing.T, workloads map[string][]corev1.Container, workload, container string) corev1.Container {
	t.Helper()
	containers, ok := workloads[workload]
	if !ok {
		t.Fatalf("workload %s not found in ate-system install", workload)
	}
	idx := slices.IndexFunc(containers, func(c corev1.Container) bool {
		return c.Name == container
	})
	if idx < 0 {
		t.Fatalf("container %q not found in workload %s", container, workload)
	}
	return containers[idx]
}

// checkPprofContainer verifies whether c exposes TCP port pprof:6060 and the
// expected pprof/contention environment variables without duplicates or extras.
func checkPprofContainer(t *testing.T, workload string, c corev1.Container, wantPort bool, wantEnv map[string]string) {
	t.Helper()
	hasValidPort := slices.ContainsFunc(c.Ports, func(p corev1.ContainerPort) bool {
		return p.Name == "pprof" && p.ContainerPort == 6060 && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP)
	})
	if wantPort && !hasValidPort {
		t.Errorf("%s container %q missing TCP port pprof:6060; got ports %+v", workload, c.Name, c.Ports)
	}
	if !wantPort {
		for _, p := range c.Ports {
			if p.Name == "pprof" || p.ContainerPort == 6060 {
				t.Errorf("%s container %q unexpectedly exposes port %+v", workload, c.Name, p)
			}
		}
	}

	envs := make(map[string]string, len(c.Env))
	for _, e := range c.Env {
		if _, dup := envs[e.Name]; dup {
			t.Errorf("%s container %q has duplicate env var %q", workload, c.Name, e.Name)
		}
		envs[e.Name] = e.Value
		isPprofEnv := strings.HasPrefix(e.Name, "ATE_PPROF_") ||
			strings.HasPrefix(e.Name, "ATE_WORKER_PPROF") ||
			e.Name == "GRPC_GO_SERVER_GOROUTINE_LABELS"
		if _, expected := wantEnv[e.Name]; isPprofEnv && !expected {
			t.Errorf("%s container %q has unexpected env var %s=%q", workload, c.Name, e.Name, e.Value)
		}
	}
	for k, want := range wantEnv {
		got, ok := envs[k]
		if !ok {
			t.Errorf("%s container %q missing env var %s (want %q)", workload, c.Name, k, want)
		} else if got != want {
			t.Errorf("%s container %q env %s = %q, want %q", workload, c.Name, k, got, want)
		}
	}
}

// checkObservabilityPolicy verifies ate-system-observability ingress rules,
// pod exclusions, and NetworkPolicy coverage across all rendered pods.
func checkObservabilityPolicy(t *testing.T, in systemInstall, wantPprof bool) {
	t.Helper()
	obs := findPolicy(t, in.policies, "ate-system-observability")
	if len(obs.Spec.Ingress) != 1 {
		t.Fatalf("ate-system-observability ingress rule count = %d, want 1", len(obs.Spec.Ingress))
	}
	if len(obs.Spec.Ingress[0].From) != 0 {
		t.Errorf("ate-system-observability ingress[0].From = %+v, want empty", obs.Spec.Ingress[0].From)
	}

	hasPprofRule := slices.ContainsFunc(obs.Spec.Ingress[0].Ports, func(pp networkingv1.NetworkPolicyPort) bool {
		return pp.Port != nil && pp.Port.StrVal == "pprof" && (pp.Protocol == nil || *pp.Protocol == corev1.ProtocolTCP)
	})
	if wantPprof && !hasPprofRule {
		t.Errorf("ate-system-observability ingress[0].Ports missing TCP named port pprof")
	}
	if !wantPprof && hasPprofRule {
		t.Errorf("ate-system-observability ingress[0].Ports unexpectedly admits port pprof")
	}

	sel := mustSelector(t, &obs.Spec.PodSelector)
	if sel.Matches(labels.Set(in.pod(t, "k8s-credential-provider").labels)) {
		t.Errorf("ate-system-observability PodSelector selects k8s-credential-provider")
	}

	resolved := 0
	for _, pod := range in.pods {
		for _, cp := range pod.ports {
			unadmitted := slices.Contains(unadmittedPorts[pod.labels["app"]], cp.Name)
			rules := in.admittingRules(t, pod, cp)
			if unadmitted && len(rules) > 0 {
				t.Errorf("%s port %d (%s) is admitted by NetworkPolicy, want unadmitted", pod.workload, cp.ContainerPort, cp.Name)
			}
			if !unadmitted && len(rules) == 0 {
				t.Errorf("%s port %d (%s) is dropped by NetworkPolicy default deny, want admitted", pod.workload, cp.ContainerPort, cp.Name)
			}
			if wantPprof && cp.Name == "pprof" && sel.Matches(labels.Set(pod.labels)) {
				resolved++
				if cp.ContainerPort != 6060 {
					t.Errorf("%s containerPort for pprof = %d, want 6060", pod.workload, cp.ContainerPort)
				}
			}
		}
	}
	if wantPprof && resolved == 0 {
		t.Errorf("ate-system-observability port pprof resolved on 0 selected pods, want >= 1")
	}
}

// checkWorkloads verifies pprof and contention settings on active Go targets
// while ensuring excluded workloads (agentgateway, fake-atelet, credential
// provider) remain untouched.
func checkWorkloads(t *testing.T, workloads map[string][]corev1.Container, cfg config.Config, wantPprof, wantContention bool) {
	t.Helper()

	credContainers := workloads["Deployment/k8s-credential-provider"]
	if len(credContainers) == 0 {
		t.Errorf("Deployment/k8s-credential-provider has 0 containers, want >= 1")
	}
	for _, c := range credContainers {
		checkPprofContainer(t, "Deployment/k8s-credential-provider", c, false, nil)
	}

	grpcEnv := map[string]string{}
	ctrlEnv := map[string]string{}
	if wantPprof {
		grpcEnv["ATE_PPROF_LISTEN_ADDR"] = ":6060"
		grpcEnv["GRPC_GO_SERVER_GOROUTINE_LABELS"] = "grpc.method=true"
		ctrlEnv["ATE_PPROF_LISTEN_ADDR"] = ":6060"
		ctrlEnv["ATE_WORKER_PPROF"] = "true"
	}
	if wantContention {
		grpcEnv["ATE_PPROF_MUTEX_FRACTION"] = "10"
		grpcEnv["ATE_PPROF_BLOCK_RATE"] = "1000000"
		ctrlEnv["ATE_PPROF_MUTEX_FRACTION"] = "10"
		ctrlEnv["ATE_PPROF_BLOCK_RATE"] = "1000000"
		ctrlEnv["ATE_WORKER_PPROF_MUTEX_FRACTION"] = "10"
		ctrlEnv["ATE_WORKER_PPROF_BLOCK_RATE"] = "1000000"
	}

	targets := []struct {
		workload  string
		container string
		wantEnv   map[string]string
		skip      bool
	}{
		{"Deployment/ate-api-server", "ate-api-server", grpcEnv, false},
		{"Deployment/ate-controller", "ate-controller", ctrlEnv, false},
		{"DaemonSet/atelet", "atelet", grpcEnv, cfg.FakeDataPlane},
		{"Deployment/atenet-router", "atenet-router", grpcEnv, cfg.Router == config.RouterAgentgateway},
		{"Deployment/atenet-egress", "ext-proc", grpcEnv, cfg.Router == config.RouterAgentgateway},
	}

	for _, tgt := range targets {
		if tgt.skip {
			containers := workloads[tgt.workload]
			if len(containers) == 0 {
				t.Errorf("workload %s has 0 containers, want >= 1", tgt.workload)
				continue
			}
			if strings.HasPrefix(tgt.workload, "Deployment/atenet-") {
				if len(containers) != 1 || containers[0].Name != "agentgateway" {
					names := make([]string, len(containers))
					for i, c := range containers {
						names[i] = c.Name
					}
					t.Errorf("%s containers = %v, want [agentgateway]", tgt.workload, names)
				}
			}
			for _, c := range containers {
				checkPprofContainer(t, tgt.workload, c, false, nil)
			}
			continue
		}
		c := findContainer(t, workloads, tgt.workload, tgt.container)
		checkPprofContainer(t, tgt.workload, c, wantPprof, tgt.wantEnv)
	}
}

// checkBaselinePreserved ensures pprof patches append rather than overwrite any
// baseline container ports or environment variables.
func checkBaselinePreserved(t *testing.T, got, baseline map[string][]corev1.Container) {
	t.Helper()
	for workload, baseContainers := range baseline {
		gotContainers, ok := got[workload]
		if !ok {
			t.Errorf("workload %s missing after applying pprof component", workload)
			continue
		}
		for _, bc := range baseContainers {
			idx := slices.IndexFunc(gotContainers, func(gc corev1.Container) bool {
				return gc.Name == bc.Name
			})
			if idx < 0 {
				t.Errorf("%s container %q missing after applying pprof component", workload, bc.Name)
				continue
			}
			gc := gotContainers[idx]
			for _, bp := range bc.Ports {
				if !slices.Contains(gc.Ports, bp) {
					t.Errorf("%s container %q dropped baseline port %+v", workload, bc.Name, bp)
				}
			}
			for _, be := range bc.Env {
				preserved := slices.ContainsFunc(gc.Env, func(ge corev1.EnvVar) bool {
					return reflect.DeepEqual(ge, be)
				})
				if !preserved {
					t.Errorf("%s container %q dropped baseline env var %q", workload, bc.Name, be.Name)
				}
			}
		}
	}
}

// TestPprofComponents verifies pprof and pprof-contention across Envoy,
// Agentgateway, kind, and fake-data-plane installs, including NetworkPolicy
// coverage and baseline port/env preservation.
func TestPprofComponents(t *testing.T) {
	cases := []struct {
		name           string
		cfg            config.Config
		components     []string
		wantPprof      bool
		wantContention bool
		checkBaseline  bool
	}{
		{
			name:          "gke-envoy",
			cfg:           config.Config{Router: config.RouterEnvoy},
			components:    []string{pprofComponentDir},
			wantPprof:     true,
			checkBaseline: true,
		},
		{
			name:       "gke-envoy-fake",
			cfg:        config.Config{Router: config.RouterEnvoy, FakeDataPlane: true},
			components: []string{pprofComponentDir},
			wantPprof:  true,
		},
		{
			name:       "gke-agentgateway",
			cfg:        config.Config{Router: config.RouterAgentgateway},
			components: []string{pprofComponentDir},
			wantPprof:  true,
		},
		{
			name:       "kind-envoy",
			cfg:        config.Config{Router: config.RouterEnvoy, Kind: true},
			components: []string{pprofComponentDir},
			wantPprof:  true,
		},
		{
			name:           "gke-envoy-contention",
			cfg:            config.Config{Router: config.RouterEnvoy},
			components:     []string{pprofComponentDir, pprofContentionComponentDir},
			wantPprof:      true,
			wantContention: true,
		},
		{
			name:           "contention-only",
			cfg:            config.Config{Router: config.RouterEnvoy},
			components:     []string{pprofContentionComponentDir},
			wantPprof:      false,
			wantContention: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, workloads := loadPprofSystemInstall(t, tc.cfg, tc.components...)
			checkObservabilityPolicy(t, in, tc.wantPprof)
			checkWorkloads(t, workloads, tc.cfg, tc.wantPprof, tc.wantContention)
			if tc.checkBaseline {
				_, baseline := loadPprofSystemInstall(t, tc.cfg)
				checkBaselinePreserved(t, workloads, baseline)
			}
		})
	}
}

// TestPprofComponentsContract checks that both pprof component manifests follow
// the ate-setup Component rules and compose cleanly onto a stand-in manifest.
func TestPprofComponentsContract(t *testing.T) {
	root := repoRoot(t)
	standIn := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: stand-in\n")

	for _, rel := range []string{pprofComponentDir, pprofContentionComponentDir} {
		t.Run(filepath.Base(rel), func(t *testing.T) {
			compDir := filepath.Join(root, rel)
			entries, err := os.ReadDir(compDir)
			if err != nil {
				t.Fatalf("reading %s: %v", compDir, err)
			}
			if len(entries) != 1 || !slices.Contains(konfig.RecognizedKustomizationFileNames(), entries[0].Name()) {
				t.Fatalf("%s has %d entries (want 1 recognized kustomization file)", rel, len(entries))
			}

			manifestPath := filepath.Join(compDir, entries[0].Name())
			raw, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatalf("reading %s: %v", manifestPath, err)
			}

			if !strings.Contains(string(raw), "Never install this on a cluster that serves real actors") {
				t.Errorf("%s missing production warning comment", rel)
			}

			var k types.Kustomization
			if err := k.Unmarshal(raw); err != nil {
				t.Fatalf("unmarshaling %s: %v", manifestPath, err)
			}
			if k.Kind != types.ComponentKind {
				t.Errorf("%s kind = %q, want %q", rel, k.Kind, types.ComponentKind)
			}
			if deprecated := k.CheckDeprecatedFields(); deprecated != nil && len(*deprecated) > 0 {
				t.Errorf("%s uses deprecated kustomize fields: %v", rel, *deprecated)
			}
			if len(k.Resources) > 0 || len(k.ConfigMapGenerator) > 0 || len(k.SecretGenerator) > 0 ||
				k.NamePrefix != "" || k.NameSuffix != "" || k.Namespace != "" ||
				len(k.Replacements) > 0 || len(k.Replicas) > 0 || len(k.Generators) > 0 ||
				len(k.Transformers) > 0 || len(k.HelmCharts) > 0 ||
				len(k.HelmChartInflationGenerator) > 0 || len(k.Components) > 0 {
				t.Errorf("%s sets forbidden top-level Kustomization fields; components may only declare patches", rel)
			}
			for i, l := range k.Labels {
				if l.IncludeSelectors {
					t.Errorf("%s labels[%d] sets includeSelectors=true, want false", rel, i)
				}
			}
			if len(k.Patches) == 0 {
				t.Errorf("%s declares 0 patches, want >= 1", rel)
			}
			for i, p := range k.Patches {
				if p.Target == nil {
					t.Errorf("%s patch[%d] has nil target", rel, i)
				}
			}

			out, err := kustomize.ComposeBytes(standIn, compDir)
			if err != nil {
				t.Fatalf("kustomize.ComposeBytes(%s): %v", rel, err)
			}
			if !strings.Contains(string(out), "stand-in") {
				t.Errorf("kustomize.ComposeBytes(%s) dropped stand-in ConfigMap", rel)
			}
		})
	}
}
