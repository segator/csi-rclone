package rclone

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/client/conditions"
	"k8s.io/utils/exec"
	"k8s.io/utils/pointer"
	"os"
	"reflect"
	"strings"
	"time"
)

type Operations interface {
	CreateVol(ctx context.Context, volumeName, remote, remotePath, rcloneConfigPath string) error
	DeleteVol(ctx context.Context, rcloneVolume *RcloneVolume, rcloneConfigPath string) error
	Mount(ctx context.Context, rcloneVolume *RcloneVolume, targetPath string, rcloneConfigData string, pameters map[string]string) error
	Unmount(ctx context.Context, rcloneVolume *RcloneVolume) error
	CleanupMountPoint(ctx context.Context, secrets, pameters map[string]string) error
	GetVolumeById(ctx context.Context, volumeId string) (*RcloneVolume, error)
}

type Rclone struct {
	execute    exec.Interface
	kubeClient *kubernetes.Clientset
	namespace  string
}

type RcloneVolume struct {
	Remote     string
	RemotePath string
	ID         string
}

func (r *Rclone) Mount(ctx context.Context, rcloneVolume *RcloneVolume, targetPath, rcloneConfigData string, parameters map[string]string) error {
	//mountingTargetPath := filepath.Dir(targetPath)
	//mountingFolderName := filepath.Base(targetPath)
	mountArgs := []string{}
	//containerMountPath := fmt.Sprintf("/mount/%s", mountingFolderName)
	mountArgs = append(mountArgs, "mount")
	mountArgs = append(mountArgs, fmt.Sprintf("%s:/%s", rcloneVolume.Remote, rcloneVolume.RemotePath))
	mountArgs = append(mountArgs, targetPath)
	defaultFlags := map[string]string{}
	defaultFlags["rc"] = ""
	defaultFlags["rc-addr"] = "0.0.0.0:5572"
	defaultFlags["rc-enable-metrics"] = ""
	defaultFlags["rc-web-gui"] = ""
	defaultFlags["rc-web-gui-no-open-browser"] = ""
	defaultFlags["rc-no-auth"] = ""
	defaultFlags["volname"] = rcloneVolume.ID
	defaultFlags["devname"] = rcloneVolume.ID
	defaultFlags["cache-info-age"] = "72h"
	defaultFlags["cache-chunk-clean-interval"] = "15m"
	defaultFlags["dir-cache-time"] = "60s"
	defaultFlags["vfs-cache-mode"] = "full"
	defaultFlags["vfs-write-back"] = "10s"
	defaultFlags["vfs-cache-max-size"] = "1g"

	defaultFlags["allow-other"] = "true"
	defaultFlags["allow-non-empty"] = "true"
	// Add default flags
	for k, v := range defaultFlags {
		// Exclude overriden flags
		if _, ok := parameters[k]; !ok {
			if v != "" {
				mountArgs = append(mountArgs, fmt.Sprintf("--%s=%s", k, v))
			} else {
				mountArgs = append(mountArgs, fmt.Sprintf("--%s", k))
			}
		}
	}

	// Add user supplied flags
	for k, v := range parameters {
		if v != "" {
			mountArgs = append(mountArgs, fmt.Sprintf("--%s=%s", k, v))
		} else {
			mountArgs = append(mountArgs, fmt.Sprintf("--%s", k))
		}
	}

	// create target, os.Mkdirall is noop if it exists
	err := os.MkdirAll(targetPath, 0750)
	if err != nil {
		return err
	}

	//deploymentName := fmt.Sprintf("%s%d", rcloneVolume.deploymentName(), uuid.New().ID())
	deploymentName := rcloneVolume.deploymentName()
	h := sha256.New()
	h.Write([]byte(rcloneConfigData))
	secretHash := hex.EncodeToString(h.Sum(nil))[:63]
	mountPropagation := corev1.MountPropagationBidirectional
	hostPathCreate := corev1.HostPathDirectoryOrCreate
	pvDeploymentLabels := map[string]string{
		"volumeid": rcloneVolume.ID,
		"hash":     secretHash,
	}

	secret, err := r.kubeClient.CoreV1().Secrets(r.namespace).Get(deploymentName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}

	if !reflect.DeepEqual(secret.Labels, pvDeploymentLabels) {
		err = r.kubeClient.CoreV1().Secrets(r.namespace).Delete(deploymentName, &metav1.DeleteOptions{})
		if err != nil && !k8serrors.IsNotFound(err) {
			return err
		}

		_, err = r.kubeClient.CoreV1().Secrets(r.namespace).Create(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: r.namespace,
				Labels:    pvDeploymentLabels,
			},
			StringData: map[string]string{
				"rclone.conf": rcloneConfigData,
			},
			Type: corev1.SecretTypeOpaque,
		})

		if err != nil {
			return err
		}
	}

	deployment, err := r.kubeClient.AppsV1().Deployments(r.namespace).Get(deploymentName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}

	if !reflect.DeepEqual(deployment.Labels, pvDeploymentLabels) {
		err = r.kubeClient.AppsV1().Deployments(r.namespace).Delete(deploymentName, &metav1.DeleteOptions{})
		if err != nil && !k8serrors.IsNotFound(err) {
			return err
		}

		r.kubeClient.AppsV1().Deployments(r.namespace).Delete(deploymentName, &metav1.DeleteOptions{})
		_, err = r.kubeClient.AppsV1().Deployments(r.namespace).Create(&v1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: r.namespace,
				Labels:    pvDeploymentLabels,
			},
			Spec: v1.DeploymentSpec{
				Replicas: pointer.Int32Ptr(1),
				Selector: &metav1.LabelSelector{
					MatchLabels: pvDeploymentLabels,
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: pvDeploymentLabels,
					},
					Spec: corev1.PodSpec{
						NodeName:                      os.Getenv("NODE_ID"),
						RestartPolicy:                 corev1.RestartPolicyAlways,
						PriorityClassName:             "system-cluster-critical",
						TerminationGracePeriodSeconds: pointer.Int64Ptr(10),
						Volumes: []corev1.Volume{
							{
								Name: "mount",
								VolumeSource: corev1.VolumeSource{
									HostPath: &corev1.HostPathVolumeSource{
										Path: targetPath,
										Type: &hostPathCreate,
									},
								},
							},
							{
								Name: "config",
								VolumeSource: corev1.VolumeSource{
									Secret: &corev1.SecretVolumeSource{
										SecretName: deploymentName,
										Items: []corev1.KeyToPath{
											{
												Key:  "rclone.conf",
												Path: "rclone.conf",
												Mode: pointer.Int32Ptr(0777),
											},
										},
										Optional: pointer.BoolPtr(false),
									},
								},
							},
						},
						Containers: []corev1.Container{
							{
								Name:    "rclone-mounter",
								Image:   "rclone/rclone:1.59.2",
								Command: []string{"rclone"},
								Args:    mountArgs,
								Ports: []corev1.ContainerPort{
									{
										Name:          "api",
										ContainerPort: 5572,
										Protocol:      "TCP",
									},
								},
								Lifecycle: &corev1.Lifecycle{
									PreStop: &corev1.Handler{
										Exec: &corev1.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
											"umount", targetPath)}},
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "config",
										MountPath: "/root/.config/rclone/",
									},
									{
										Name:             "mount",
										MountPath:        targetPath,
										MountPropagation: &mountPropagation,
									},
								},
								SecurityContext: &corev1.SecurityContext{
									Capabilities: &corev1.Capabilities{
										Add: []corev1.Capability{"SYS_ADMIN"},
									},
									Privileged: pointer.BoolPtr(true),
								},
								LivenessProbe: &corev1.Probe{
									InitialDelaySeconds: 1,
									TimeoutSeconds:      5,
									PeriodSeconds:       10,
									SuccessThreshold:    1,
									FailureThreshold:    10,
									Handler: corev1.Handler{
										Exec: &corev1.ExecAction{
											Command: []string{"sh", "-c", fmt.Sprintf("ls -lah %s", targetPath)},
										},
									},
								},
								ReadinessProbe: &corev1.Probe{
									InitialDelaySeconds: 1,
									TimeoutSeconds:      5,
									PeriodSeconds:       10,
									SuccessThreshold:    1,
									FailureThreshold:    10,
									Handler: corev1.Handler{
										HTTPGet: &corev1.HTTPGetAction{
											Path: "/metrics",
											Port: intstr.FromInt(5572),
										},
									},
								},
							},
						},
					},
				},
				Strategy: v1.DeploymentStrategy{
					Type: v1.RecreateDeploymentStrategyType,
				},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func ListSecretsByLabel(client *kubernetes.Clientset, namespace string, lab map[string]string) (*corev1.SecretList, error) {
	return client.CoreV1().Secrets(namespace).List(metav1.ListOptions{
		LabelSelector: labels.FormatLabels(lab),
	})
}

func DeleteSecretsByLabel(client *kubernetes.Clientset, namespace string, lab map[string]string) error {
	//propagation := metav1.DeletePropagationBackground
	return client.CoreV1().Secrets(namespace).DeleteCollection(&metav1.DeleteOptions{
		//PropagationPolicy: &propagation,
	},
		metav1.ListOptions{
			LabelSelector: labels.FormatLabels(lab),
		})
}

func DeleteDeploymentByLabel(client *kubernetes.Clientset, namespace string, lab map[string]string) error {
	propagation := metav1.DeletePropagationForeground
	return client.AppsV1().Deployments(namespace).DeleteCollection(&metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	},
		metav1.ListOptions{
			LabelSelector: labels.FormatLabels(lab),
		})
}

func (r *RcloneVolume) normalizedVolumeId() string {
	return strings.ToLower(strings.ReplaceAll(r.ID, ":", "-"))
}
func (r *RcloneVolume) deploymentName() string {
	volumeID := fmt.Sprintf("rclone-mounter-%s", r.ID)
	if len(volumeID) > 63 {
		volumeID = volumeID[:63]
	}

	return strings.ToLower(volumeID)
}

func (r *Rclone) CreateVol(ctx context.Context, volumeName, remote, remotePath, rcloneConfigPath string) error {
	// Create subdirectory under base-dir
	path := fmt.Sprintf("%s/%s", remotePath, volumeName)
	return r.command("mkdir", remote, path, map[string]string{
		"config": rcloneConfigPath,
	})
}

func (r Rclone) DeleteVol(ctx context.Context, rcloneVolume *RcloneVolume, rcloneConfigPath string) error {
	return r.command("rmdirs", rcloneVolume.Remote, rcloneVolume.RemotePath, map[string]string{
		"config": rcloneConfigPath,
	})
}

func (r Rclone) Unmount(ctx context.Context, rcloneVolume *RcloneVolume) error {
	deploymentName := rcloneVolume.deploymentName()
	err := r.kubeClient.AppsV1().Deployments(r.namespace).Delete(deploymentName, &metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	return r.kubeClient.CoreV1().Secrets(r.namespace).Delete(deploymentName, &metav1.DeleteOptions{})

	/*	labelQuery := map[string]string{
			"volumeid": rcloneVolume.ID,
		}
		err := DeleteDeploymentByLabel(r.kubeClient, r.namespace, labelQuery)
		if err != nil {
			return err
		}
		return DeleteSecretsByLabel(r.kubeClient, r.namespace, labelQuery)*/
}

func (r Rclone) CleanupMountPoint(ctx context.Context, secrets, pameters map[string]string) error {
	//TODO implement me
	panic("implement me")
}

func (r Rclone) GetVolumeById(ctx context.Context, volumeId string) (*RcloneVolume, error) {
	pvs, err := r.kubeClient.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, pv := range pvs.Items {
		if pv.Spec.CSI == nil {
			continue
		}
		if pv.Spec.CSI.VolumeHandle == volumeId {
			remote := pv.Spec.CSI.VolumeAttributes["remote"]
			if remote == "" {
				return nil, errors.New("Missing remote volume attribute")
			}
			path := pv.Spec.CSI.VolumeAttributes["path"]
			if path == "" {
				return nil, errors.New("Missing path volume attribute")
			}

			return &RcloneVolume{
				Remote:     remote,
				RemotePath: path,
				ID:         volumeId,
			}, nil
		}
	}
	return nil, fmt.Errorf("volume %s not found", volumeId)
}

func NewRclone(kubeClient *kubernetes.Clientset) Operations {
	return &Rclone{
		execute:    exec.New(),
		kubeClient: kubeClient,
		namespace:  os.Getenv("POD_NAMESPACE"),
	}
}

func (r *Rclone) command(cmd, remote, remotePath string, flags map[string]string) error {
	// rclone <operand> remote:path [flag]
	args := append(
		[]string{},
		cmd,
		fmt.Sprintf("%s:%s", remote, remotePath),
	)

	// Add user supplied flags
	for k, v := range flags {
		args = append(args, fmt.Sprintf("--%s=%s", k, v))
	}

	klog.Infof("executing %s command cmd=rclone, remote=%s:%s", cmd, remote, remotePath)
	out, err := r.execute.Command("rclone", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s failed: %v cmd: 'rclone' remote: ':%s:%s' output: %q",
			cmd, err, remote, remotePath, string(out))
	}

	return nil
}

func WaitForPodBySelectorRunning(c kubernetes.Interface, namespace, selector string, timeout int) error {
	podList, err := ListPods(c, namespace, selector)
	if err != nil {
		return err
	}
	if len(podList.Items) == 0 {
		return fmt.Errorf("no pods in %s with selector %s", namespace, selector)
	}

	for _, pod := range podList.Items {
		if err := waitForPodRunning(c, namespace, pod.Name, time.Duration(timeout)*time.Second); err != nil {
			return err
		}
	}
	return nil
}
func ListPods(c kubernetes.Interface, namespace, selector string) (*corev1.PodList, error) {
	listOptions := metav1.ListOptions{IncludeUninitialized: true, LabelSelector: selector}
	podList, err := c.CoreV1().Pods(namespace).List(listOptions)

	if err != nil {
		return nil, err
	}
	return podList, nil
}

func waitForPodRunning(c kubernetes.Interface, namespace, podName string, timeout time.Duration) error {
	return wait.PollImmediate(time.Second, timeout, isPodRunning(c, podName, namespace))
}

func isPodRunning(c kubernetes.Interface, podName, namespace string) wait.ConditionFunc {
	return func() (bool, error) {
		fmt.Printf(".") // progress bar!

		pod, err := c.CoreV1().Pods(namespace).Get(podName, metav1.GetOptions{IncludeUninitialized: true})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case corev1.PodRunning:
			return true, nil
		case corev1.PodFailed, corev1.PodSucceeded:
			return false, conditions.ErrPodCompleted
		}
		return false, nil
	}
}
