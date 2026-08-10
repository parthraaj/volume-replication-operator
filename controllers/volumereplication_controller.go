/*
Copyright 2025.

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

package controllers

import (
	"context"
	"fmt"
	"time"

	volumegroupv1 "github.com/IBM/csi-volume-group-operator/api/v1"

	replicationv1alpha1 "github.com/csi-addons/volume-replication-operator/api/v1alpha1"
	"github.com/csi-addons/volume-replication-operator/controllers/replication"
	grpcClient "github.com/csi-addons/volume-replication-operator/pkg/client"
	"github.com/csi-addons/volume-replication-operator/pkg/config"

	replicationlib "github.com/csi-addons/spec/lib/go/replication"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	pvcDataSource          = "PersistentVolumeClaim"
	volumeGroupDataSource  = "VolumeGroup"
	volumeReplicationClass = "VolumeReplicationClass"
	volumeReplication      = "VolumeReplication"
	defaultScheduleTime    = time.Hour
)

var (
	volumePromotionKnownErrors               = []codes.Code{codes.FailedPrecondition}
	disableReplicationKnownErrors            = []codes.Code{codes.NotFound}
	getReplicationInfoKnownErrors            = []codes.Code{codes.NotFound}
	getReplicationDestinationInfoKnownErrors = []codes.Code{codes.NotFound, codes.Unimplemented}
	promoteRemoteNotReadyErrors              = []codes.Code{codes.Internal}
)

// VolumeReplicationReconciler reconciles a VolumeReplication object.
type VolumeReplicationReconciler struct {
	client.Client

	Log          logr.Logger
	Scheme       *runtime.Scheme
	DriverConfig *config.DriverConfig
	GRPCClient   *grpcClient.Client
	Replication  grpcClient.VolumeReplication
}

// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumereplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumereplications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=replication.storage.openshift.io,resources=volumereplications/finalizers,verbs=update
// +kubebuilder:rbac:groups=ramendr.openshift.io,resources=volumereplicationgroups,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
func (r *VolumeReplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("Request.Name", req.Name, "Request.Namespace", req.Namespace)

	// Fetch VolumeReplication instance
	instance := &replicationv1alpha1.VolumeReplication{}

	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			logger.Info("volumeReplication resource not found")

			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Get VolumeReplicationClass
	vrcObj, err := r.getVolumeReplicationClass(ctx, logger, instance.Spec.VolumeReplicationClass)
	if err != nil {
		setFailureCondition(instance)

		uErr := r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), err.Error())
		if uErr != nil {
			logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
		}

		return ctrl.Result{}, err
	}

	if r.DriverConfig.DriverName != vrcObj.Spec.Provisioner {
		return ctrl.Result{}, nil
	}

	err = validatePrefixedParameters(vrcObj.Spec.Parameters)
	if err != nil {
		logger.Error(err, "failed to validate parameters of volumeReplicationClass", "VRCName", instance.Spec.VolumeReplicationClass)
		setFailureCondition(instance)

		uErr := r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), err.Error())
		if uErr != nil {
			logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
		}

		return ctrl.Result{}, err
	}
	// remove the prefix keys in volume replication class parameters
	parameters := filterPrefixedParameters(replicationParameterPrefix, vrcObj.Spec.Parameters)

	// get secret
	secretName := vrcObj.Spec.Parameters[prefixedReplicationSecretNameKey]
	secretNamespace := vrcObj.Spec.Parameters[prefixedReplicationSecretNamespaceKey]

	secret := make(map[string]string)
	if secretName != "" && secretNamespace != "" {
		secret, err = r.getSecret(ctx, logger, secretName, secretNamespace)
		if err != nil {
			setFailureCondition(instance)

			uErr := r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), err.Error())
			if uErr != nil {
				logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
			}

			return reconcile.Result{}, err
		}
	}

	var (
		volumeHandle string
		pvc          *corev1.PersistentVolumeClaim
		pv           *corev1.PersistentVolume
		vg           *volumegroupv1.VolumeGroup
		vgc          *volumegroupv1.VolumeGroupContent
		pvErr        error
		vgErr        error
	)

	replicationHandle := instance.Spec.ReplicationHandle

	nameSpacedName := types.NamespacedName{Name: instance.Spec.DataSource.Name, Namespace: req.Namespace}
	switch instance.Spec.DataSource.Kind {
	case pvcDataSource:
		pvc, pv, pvErr = r.getPVCDataSource(ctx, logger, nameSpacedName)
		if pvErr != nil {
			logger.Error(pvErr, "failed to get PVC", "PVCName", instance.Spec.DataSource.Name)
			setFailureCondition(instance)

			uErr := r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), pvErr.Error())
			if uErr != nil {
				logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
			}

			return ctrl.Result{}, pvErr
		}

		volumeHandle = pv.Spec.CSI.VolumeHandle
	case volumeGroupDataSource:
		vg, vgc, vgErr = r.getVGDataSource(ctx, logger, nameSpacedName)
		if vgErr != nil {
			logger.Error(vgErr, "failed to get VG", "VGName", instance.Spec.DataSource.Name)
			setFailureCondition(instance)

			uErr := r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), vgErr.Error())
			if uErr != nil {
				logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
			}

			return ctrl.Result{}, vgErr
		}

		volumeHandle = vgc.Spec.Source.VolumeGroupHandle
	default:
		err = fmt.Errorf("unsupported datasource kind")
		logger.Error(err, "given kind not supported", "Kind", instance.Spec.DataSource.Kind)
		setFailureCondition(instance)

		uErr := r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), err.Error())
		if uErr != nil {
			logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
		}

		return ctrl.Result{}, nil
	}

	logger.Info("volume handle", "VolumeHandleName", volumeHandle)
	replicationSource := r.getReplicationSource(instance.Spec.DataSource.Kind, volumeHandle)
	logger.Info("Replication source", "replicationSource", replicationSource)

	if replicationHandle != "" {
		logger.Info("Replication handle", "ReplicationHandleName", replicationHandle)
	}

	// check if the object is being deleted
	if instance.GetDeletionTimestamp().IsZero() {
		err = r.addFinalizerToVR(ctx, logger, instance)
		if err != nil {
			logger.Error(err, "Failed to add VolumeReplication finalizer")

			return reconcile.Result{}, err
		}

		if pvc != nil {
			err = r.addFinalizerToPVC(ctx, logger, pvc)
			if err != nil {
				logger.Error(err, "Failed to add PersistentVolumeClaim finalizer")

				return reconcile.Result{}, err
			}
		}

		if vg != nil {
			err = r.addFinalizerToVG(ctx, logger, vg)
			if err != nil {
				logger.Error(err, "Failed to add VolumeGroup finalizer")

				return reconcile.Result{}, err
			}
		}
	} else {
		if contains(instance.GetFinalizers(), volumeReplicationFinalizer) {

			// If the user's desired state is Secondary OR the storage is currently in a Secondary state,
			// skip the gRPC call to DisableVolumeReplication entirely.
			if instance.Spec.ReplicationState == replicationv1alpha1.Secondary ||
				instance.Status.State == replicationv1alpha1.SecondaryState {

				logger.Info("Skipping DisableVolumeReplication gRPC call: VR object is in Secondary state",
					"VRName", instance.Name,
					"SpecState", instance.Spec.ReplicationState,
					"StatusState", instance.Status.State)

			} else {
				err = r.disableVolumeReplication(logger, replicationSource, replicationHandle, parameters, secret)
				if err != nil {
					logger.Error(err, "failed to disable replication")

					return ctrl.Result{}, err
				}
			}

			if pvc != nil {
				err = r.removeFinalizerFromPVC(ctx, logger, pvc)
				if err != nil {
					logger.Error(err, "Failed to remove PersistentVolumeClaim finalizer")

					return reconcile.Result{}, err
				}
			}

			if vg != nil {
				err = r.removeFinalizerFromVG(ctx, logger, vg)
				if err != nil {
					logger.Error(err, "Failed to remove VolumeGroup finalizer")

					return reconcile.Result{}, err
				}
			}
			// once all finalizers have been removed, the object will be
			// deleted
			err = r.removeFinalizerFromVR(ctx, logger, instance)
			if err != nil {
				logger.Error(err, "Failed to remove VolumeReplication finalizer")

				return reconcile.Result{}, err
			}
		}

		logger.Info("volumeReplication object is terminated, skipping reconciliation")

		return ctrl.Result{}, nil
	}

	instance.Status.LastStartTime = getCurrentTime()

	err = r.Update(ctx, instance)
	if err != nil {
		logger.Error(err, "failed to update status")

		return reconcile.Result{}, err
	}

	// skip enable and promote gRPC calls if parent VRG desires secondary
	if instance.Spec.ReplicationState == replicationv1alpha1.Primary &&
		r.isParentVRGSecondary(ctx, instance, logger) {
		logger.Info("skipping EnableReplication and Promote: parent VRG is secondary",
			"VRName", instance.Name)
		return ctrl.Result{}, nil
	}

	// enable replication on every reconcile
	err = r.enableReplication(logger, replicationSource, replicationHandle, parameters, secret)
	if err != nil {
		logger.Error(err, "failed to enable replication")
		setFailureCondition(instance)

		msg := replication.GetMessageFromError(err)

		uErr := r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), msg)
		if uErr != nil {
			logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
		}

		return reconcile.Result{}, err
	}

	var replicationErr error

	var requeueForResync bool

	switch instance.Spec.ReplicationState {
	case replicationv1alpha1.Primary:
		replicationErr = r.markVolumeAsPrimary(instance, logger, replicationSource, replicationHandle, parameters, secret)

	case replicationv1alpha1.Secondary:
		// For the first time, mark the volume as secondary and requeue the
		// request. For some storage providers it takes some time to determine
		// whether the volume need correction example:- correcting split brain.
		if instance.Status.State != replicationv1alpha1.SecondaryState {
			replicationErr = r.markVolumeAsSecondary(instance, logger, replicationSource, replicationHandle, parameters, secret)
			if replicationErr == nil {
				logger.Info("volume is not ready to use")
				// set the status.State to secondary as the
				// instance.Status.State is primary for the first time.
				err = r.updateReplicationStatus(ctx, instance, logger, getReplicationState(instance), "volume is marked secondary and is degraded")
				if err != nil {
					return ctrl.Result{}, err
				}

				return ctrl.Result{
					Requeue: true,
					// Setting Requeue time for 15 seconds
					RequeueAfter: time.Second * 15,
				}, nil
			}
		} else {
			replicationErr = r.markVolumeAsSecondary(instance, logger, replicationSource, replicationHandle, parameters, secret)
			// resync volume if successfully marked Secondary
			if replicationErr == nil {
				requeueForResync, replicationErr = r.resyncVolume(instance, logger, replicationSource, replicationHandle, instance.Spec.AutoResync, parameters, secret)
			}
		}

	case replicationv1alpha1.Resync:
		requeueForResync, replicationErr = r.resyncVolume(instance, logger, replicationSource, replicationHandle, true, parameters, secret)

	default:
		replicationErr = fmt.Errorf("unsupported volume state")
		logger.Error(replicationErr, "given volume state is not supported", "ReplicationState", instance.Spec.ReplicationState)
		setFailureCondition(instance)

		err = r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), replicationErr.Error())
		if err != nil {
			logger.Error(err, "failed to update volumeReplication status", "VRName", instance.Name)
		}

		return ctrl.Result{}, nil
	}

	if replicationErr != nil {
		msg := replication.GetMessageFromError(replicationErr)
		logger.Error(replicationErr, "failed to Replicate", "ReplicationState", instance.Spec.ReplicationState)

		err = r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), msg)
		if err != nil {
			logger.Error(err, "failed to update volumeReplication status", "VRName", instance.Name)
		}

		if grpcStatus, ok := grpcstatus.FromError(replicationErr); ok &&
			instance.Spec.ReplicationState == replicationv1alpha1.Primary {
			for _, code := range promoteRemoteNotReadyErrors {
				if grpcStatus.Code() == code {
					logger.Info("secondary storage not ready for promotion, requeuing",
						"VRName", instance.Name,
						"RequeueAfter", "5s")
					return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
				}
			}
		}

		if instance.Status.State == replicationv1alpha1.SecondaryState {
			return ctrl.Result{
				Requeue: true,
				// in case of any error during secondary state, requeue for every 15 seconds.
				RequeueAfter: time.Second * 15,
			}, nil
		}

		return ctrl.Result{}, replicationErr
	}

	if requeueForResync {
		logger.Info("volume is not ready to use, requeuing for resync")

		err = r.updateReplicationStatus(ctx, instance, logger, getCurrentReplicationState(instance), "volume is degraded")
		if err != nil {
			logger.Error(err, "failed to update volumeReplication status", "VRName", instance.Name)
		}

		return ctrl.Result{
			Requeue: true,
			// Setting Requeue time for 30 seconds as the resync can take time
			// and having default Requeue exponential backoff time can affect
			// the RTO time.
			RequeueAfter: time.Second * 30,
		}, nil
	}

	var msg string
	if instance.Spec.ReplicationState == replicationv1alpha1.Resync {
		msg = "volume is marked for resyncing"
	} else {
		msg = fmt.Sprintf("volume is marked %s", string(instance.Spec.ReplicationState))
	}

	instance.Status.LastCompletionTime = getCurrentTime()

	requeueForInfo := false

	isRamenFlow := instance.Spec.DataSource.Kind == pvcDataSource && parameters["replication_policy"] != ""
	isTraditionalVGFlow := instance.Spec.DataSource.Kind == volumeGroupDataSource

	if instance.Spec.ReplicationState == replicationv1alpha1.Primary && (isRamenFlow || isTraditionalVGFlow) {
		info, infoErr := r.getVolumeReplicationInfo(instance, logger, replicationSource, replicationHandle, secret)
		if infoErr != nil {
			uErr := r.updateReplicationStatus(ctx, instance, logger, getReplicationState(instance), msg)
			if uErr != nil {
				logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
			}
			return ctrl.Result{}, infoErr
		}
		if info != nil {
			protoTimestamp := info.GetLastSyncTime()
			if protoTimestamp != nil {
				lastSyncTime := metav1.NewTime(protoTimestamp.AsTime())
				instance.Status.LastSyncTime = &lastSyncTime
			} else {
				instance.Status.LastSyncTime = nil
			}

			protoDuration := info.GetLastSyncDuration()
			if protoDuration != nil {
				lastSyncDuration := metav1.Duration{Duration: protoDuration.AsDuration()}
				instance.Status.LastSyncDuration = &lastSyncDuration
			} else {
				instance.Status.LastSyncDuration = nil
			}

			lastSyncBytes := info.GetLastSyncBytes()
			if lastSyncBytes != 0 {
				instance.Status.LastSyncBytes = &lastSyncBytes
			} else {
				instance.Status.LastSyncBytes = nil
			}

			instance.Status.ReplicationStatus = protoReplicationStatusToString(info.GetStatus())
			instance.Status.StatusMessage = info.GetStatusMessage()

			requeueForInfo = true
		}
	}

	if isRamenFlow {
		destInfo, destErr := r.getReplicationDestinationInfo(instance, logger, replicationSource, secret)
		if destErr != nil {
			setDestinationInfoFailedCondition(&instance.Status.Conditions, instance.Generation,
				instance.Spec.DataSource.Kind, destErr.Error())
		} else if destInfo != nil {
			replicationDest := destInfo.GetReplicationDestination()
			if replicationDest != nil {
				if volDest := replicationDest.GetVolume(); volDest != nil {
					instance.Status.DestinationVolumeID = volDest.GetVolumeId()
				}
				setDestinationInfoAvailableCondition(&instance.Status.Conditions, instance.Generation,
					instance.Spec.DataSource.Kind)
			} else {
				instance.Status.DestinationVolumeID = ""
				setDestinationInfoPendingCondition(&instance.Status.Conditions, instance.Generation,
					instance.Spec.DataSource.Kind)

				uErr := r.updateReplicationStatus(ctx, instance, logger, getReplicationState(instance), msg)
				if uErr != nil {
					logger.Error(uErr, "failed to update volumeReplication status", "VRName", instance.Name)
				}
				logger.Info("destination info pending, requeuing in 30s")
				return ctrl.Result{Requeue: true, RequeueAfter: 30 * time.Second}, nil
			}
		}
	}

	err = r.updateReplicationStatus(ctx, instance, logger, getReplicationState(instance), msg)
	if err != nil {
		return ctrl.Result{}, err
	}

	logger.Info(msg)

	if requeueForInfo {
		interval := getInfoReconcileInterval(parameters, logger)
		return ctrl.Result{Requeue: true, RequeueAfter: interval}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *VolumeReplicationReconciler) SetupWithManager(mgr ctrl.Manager, cfg *config.DriverConfig) error {
	err := r.waitForCrds()
	if err != nil {
		r.Log.Error(err, "failed to wait for crds")

		return err
	}

	r.Scheme.AddKnownTypes(volumegroupv1.GroupVersion,
		&volumegroupv1.VolumeGroup{},
		&volumegroupv1.VolumeGroupContent{},
		&volumegroupv1.VolumeGroupList{},
		&volumegroupv1.VolumeGroupContentList{},
	)
	metav1.AddToGroupVersion(r.Scheme, volumegroupv1.GroupVersion)

	pred := predicate.GenerationChangedPredicate{}

	r.DriverConfig = cfg

	gClient, err := grpcClient.New(cfg.DriverEndpoint, cfg.RPCTimeout)
	if err != nil {
		r.Log.Error(err, "failed to create GRPC Client", "Endpoint", cfg.DriverEndpoint, "GRPC Timeout", cfg.RPCTimeout)

		return err
	}

	err = gClient.Probe()
	if err != nil {
		r.Log.Error(err, "failed to connect to driver", "Endpoint", cfg.DriverEndpoint, "GRPC Timeout", cfg.RPCTimeout)

		return err
	}

	r.GRPCClient = gClient
	r.Replication = grpcClient.NewReplicationClient(r.GRPCClient.Client, cfg.RPCTimeout)

	return ctrl.NewControllerManagedBy(mgr).
		For(&replicationv1alpha1.VolumeReplication{}).
		WithEventFilter(pred).Complete(r)
}

func (r *VolumeReplicationReconciler) updateReplicationStatus(
	ctx context.Context,
	instance *replicationv1alpha1.VolumeReplication,
	logger logr.Logger,
	state replicationv1alpha1.State,
	message string,
) error {
	instance.Status.State = state
	instance.Status.Message = message
	instance.Status.ObservedGeneration = instance.Generation

	err := r.Status().Update(ctx, instance)
	if err != nil {
		logger.Error(err, "failed to update status")

		return err
	}

	return nil
}

func (r *VolumeReplicationReconciler) waitForCrds() error {
	logger := r.Log.WithName("checkingDependencies")

	err := r.waitForVolumeReplicationResource(logger, volumeReplicationClass)
	if err != nil {
		logger.Error(err, "failed to wait for VolumeReplicationClass CRD")

		return err
	}

	err = r.waitForVolumeReplicationResource(logger, volumeReplication)
	if err != nil {
		logger.Error(err, "failed to wait for VolumeReplication CRD")

		return err
	}

	return nil
}

func (r *VolumeReplicationReconciler) waitForVolumeReplicationResource(logger logr.Logger, resourceName string) error {
	unstructuredResource := &unstructured.UnstructuredList{}
	unstructuredResource.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   replicationv1alpha1.GroupVersion.Group,
		Kind:    resourceName,
		Version: replicationv1alpha1.GroupVersion.Version,
	})

	for {
		err := r.List(context.TODO(), unstructuredResource)
		if err == nil {
			return nil
		}
		// return errors other than NoMatch
		if !meta.IsNoMatchError(err) {
			logger.Error(err, "got an unexpected error while waiting for resource", "Resource", resourceName)

			return err
		}

		logger.Info("resource does not exist", "Resource", resourceName)
		time.Sleep(5 * time.Second)
	}
}

// markVolumeAsPrimary defines and runs a set of tasks required to mark a volume as primary.
func (r *VolumeReplicationReconciler) markVolumeAsPrimary(volumeReplicationObject *replicationv1alpha1.VolumeReplication,
	logger logr.Logger, replicationSource *replicationlib.ReplicationSource, replicationID string, parameters, secrets map[string]string,
) error {
	params := replication.CommonRequestParameters{
		ReplicationSource: replicationSource,
		ReplicationID:     replicationID,
		Parameters:        parameters,
		Secrets:           secrets,
		Replication:       r.Replication,
	}

	volumeReplication := replication.Replication{
		Params: params,
	}

	resp := volumeReplication.Promote()
	if resp.Error != nil {
		isKnownError := resp.HasKnownGRPCError(volumePromotionKnownErrors)
		if !isKnownError {
			if resp.Error != nil {
				logger.Error(resp.Error, "failed to promote volume")
				setFailedPromotionCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

				return resp.Error
			}
		} else {
			// force promotion
			logger.Info("force promoting volume due to known grpc error", "error", resp.Error)

			volumeReplication.Force = true

			resp := volumeReplication.Promote()
			if resp.Error != nil {
				logger.Error(resp.Error, "failed to force promote volume")
				setFailedPromotionCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

				return resp.Error
			}
		}
	}

	setPromotedCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

	return nil
}

// markVolumeAsSecondary defines and runs a set of tasks required to mark a volume as secondary.
func (r *VolumeReplicationReconciler) markVolumeAsSecondary(volumeReplicationObject *replicationv1alpha1.VolumeReplication,
	logger logr.Logger, replicationSource *replicationlib.ReplicationSource, replicationID string, parameters, secrets map[string]string,
) error {
	params := replication.CommonRequestParameters{
		ReplicationSource: replicationSource,
		ReplicationID:     replicationID,
		Parameters:        parameters,
		Secrets:           secrets,
		Replication:       r.Replication,
	}

	volumeReplication := replication.Replication{
		Params: params,
	}

	resp := volumeReplication.Demote()

	if resp.Error != nil {
		logger.Error(resp.Error, "failed to demote volume")
		setFailedDemotionCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

		return resp.Error
	}

	setDemotedCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

	return nil
}

// resyncVolume defines and runs a set of tasks required to resync the volume.
func (r *VolumeReplicationReconciler) resyncVolume(volumeReplicationObject *replicationv1alpha1.VolumeReplication,
	logger logr.Logger, replicationSource *replicationlib.ReplicationSource, replicationID string, force bool, parameters, secrets map[string]string,
) (bool, error) {
	params := replication.CommonRequestParameters{
		ReplicationSource: replicationSource,
		ReplicationID:     replicationID,
		Parameters:        parameters,
		Secrets:           secrets,
		Replication:       r.Replication,
	}

	volumeReplication := replication.Replication{
		Params: params,
		Force:  force,
	}

	resp := volumeReplication.Resync()

	if resp.Error != nil {
		logger.Error(resp.Error, "failed to resync volume")
		setFailedResyncCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

		return false, resp.Error
	}

	resyncResponse, ok := resp.Response.(*replicationlib.ResyncVolumeResponse)
	if !ok {
		err := fmt.Errorf("received response of unexpected type")
		logger.Error(err, "unable to parse response")
		setFailedResyncCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

		return false, err
	}

	setResyncCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

	if !resyncResponse.GetReady() {
		return true, nil
	}

	// No longer degraded, as volume is fully synced
	setNotDegradedCondition(&volumeReplicationObject.Status.Conditions, volumeReplicationObject.Generation)

	return false, nil
}

// disableVolumeReplication defines and runs a set of tasks required to disable volume replication.
func (r *VolumeReplicationReconciler) disableVolumeReplication(logger logr.Logger, replicationSource *replicationlib.ReplicationSource, replicationID string,
	parameters, secrets map[string]string,
) error {
	params := replication.CommonRequestParameters{
		ReplicationSource: replicationSource,
		ReplicationID:     replicationID,
		Parameters:        parameters,
		Secrets:           secrets,
		Replication:       r.Replication,
	}

	volumeReplication := replication.Replication{
		Params: params,
	}

	resp := volumeReplication.Disable()

	if resp.Error != nil {
		isKnownError := resp.HasKnownGRPCError(disableReplicationKnownErrors)
		if isKnownError {
			logger.Info("volume not found", "replicationSource", replicationSource)

			return nil
		}

		logger.Error(resp.Error, "failed to disable volume replication")

		return resp.Error
	}

	return nil
}

// enableReplication enable volume replication on the first reconcile.
func (r *VolumeReplicationReconciler) enableReplication(logger logr.Logger, replicationSource *replicationlib.ReplicationSource, replicationID string,
	parameters, secrets map[string]string,
) error {
	params := replication.CommonRequestParameters{
		ReplicationSource: replicationSource,
		ReplicationID:     replicationID,
		Parameters:        parameters,
		Secrets:           secrets,
		Replication:       r.Replication,
	}

	volumeReplication := replication.Replication{
		Params: params,
	}

	resp := volumeReplication.Enable()

	if resp.Error != nil {
		logger.Error(resp.Error, "failed to enable volume replication")

		return resp.Error
	}

	return nil
}

// getVolumeContentSource is a helper function to process provisioning requests that include a DataSource
// currently we provide Snapshot and PVC, the default case allows the provisioner to still create a volume
// so that an external controller can act upon it. Additional DataSource types can be added here with
// an appropriate implementation function.
func (r *VolumeReplicationReconciler) getReplicationSource(kind string, volumeHandle string) *replicationlib.ReplicationSource {
	switch kind {
	case pvcDataSource:
		volumeSource := replicationlib.ReplicationSource_Volume{
			Volume: &replicationlib.ReplicationSource_VolumeSource{
				VolumeId: volumeHandle,
			},
		}
		replicationSource := &replicationlib.ReplicationSource{
			Type: &volumeSource,
		}

		return replicationSource

	case volumeGroupDataSource:
		volumeGroupSource := replicationlib.ReplicationSource_Volumegroup{
			Volumegroup: &replicationlib.ReplicationSource_VolumeGroupSource{
				VolumeGroupId: volumeHandle,
			},
		}
		replicationSource := &replicationlib.ReplicationSource{
			Type: &volumeGroupSource,
		}

		return replicationSource
	default:
		// For now we shouldn't pass other things to this function, but treat it as a noop and extend as needed
		return nil
	}
}

func getReplicationState(instance *replicationv1alpha1.VolumeReplication) replicationv1alpha1.State {
	switch instance.Spec.ReplicationState {
	case replicationv1alpha1.Primary:
		return replicationv1alpha1.PrimaryState
	case replicationv1alpha1.Secondary:
		return replicationv1alpha1.SecondaryState
	case replicationv1alpha1.Resync:
		return replicationv1alpha1.SecondaryState
	}

	return replicationv1alpha1.UnknownState
}

func getCurrentReplicationState(instance *replicationv1alpha1.VolumeReplication) replicationv1alpha1.State {
	if instance.Status.State == "" {
		return replicationv1alpha1.UnknownState
	}

	return instance.Status.State
}

func setFailureCondition(instance *replicationv1alpha1.VolumeReplication) {
	switch instance.Spec.ReplicationState {
	case replicationv1alpha1.Primary:
		setFailedPromotionCondition(&instance.Status.Conditions, instance.Generation)
	case replicationv1alpha1.Secondary:
		setFailedDemotionCondition(&instance.Status.Conditions, instance.Generation)
	case replicationv1alpha1.Resync:
		setFailedResyncCondition(&instance.Status.Conditions, instance.Generation)
	}
}

func getCurrentTime() *metav1.Time {
	metav1NowTime := metav1.NewTime(time.Now())

	return &metav1NowTime
}

func (r *VolumeReplicationReconciler) getVolumeReplicationInfo(
	instance *replicationv1alpha1.VolumeReplication,
	logger logr.Logger,
	replicationSource *replicationlib.ReplicationSource,
	replicationID string,
	secrets map[string]string,
) (*replicationlib.GetVolumeReplicationInfoResponse, error) {
	params := replication.CommonRequestParameters{
		ReplicationSource: replicationSource,
		ReplicationID:     replicationID,
		Secrets:           secrets,
		Replication:       r.Replication,
		Parameters:        map[string]string{},
	}

	vr := replication.Replication{Params: params}
	resp := vr.GetInfo()
	if resp.Error != nil {
		logger.Error(resp.Error, "failed to get volume replication info", "VRName", instance.Name)
		if isKnownError := resp.HasKnownGRPCError(getReplicationInfoKnownErrors); isKnownError {
			logger.Info("volume replication info not found", "VRName", instance.Name)
			return nil, nil
		}
		return nil, resp.Error
	}

	infoResp, ok := resp.Response.(*replicationlib.GetVolumeReplicationInfoResponse)
	if !ok {
		err := fmt.Errorf("received response of unexpected type")
		logger.Error(err, "unable to parse GetVolumeReplicationInfo response", "VRName", instance.Name)
		return nil, err
	}
	return infoResp, nil
}

func (r *VolumeReplicationReconciler) getReplicationDestinationInfo(
	instance *replicationv1alpha1.VolumeReplication,
	logger logr.Logger,
	replicationSource *replicationlib.ReplicationSource,
	secrets map[string]string,
) (*replicationlib.GetReplicationDestinationInfoResponse, error) {
	params := replication.CommonRequestParameters{
		ReplicationSource: replicationSource,
		Secrets:           secrets,
		Replication:       r.Replication,
		Parameters:        map[string]string{},
	}

	vr := replication.Replication{Params: params}
	resp := vr.GetDestinationInfo()

	if resp.Error != nil {
		logger.Error(resp.Error, "failed to get replication destination info", "VRName", instance.Name)
		if isKnownError := resp.HasKnownGRPCError(getReplicationDestinationInfoKnownErrors); isKnownError {
			logger.Info("replication destination info not found or not implemented, skipping",
				"VRName", instance.Name)
			return nil, nil
		}
		return nil, resp.Error
	}

	destResp, ok := resp.Response.(*replicationlib.GetReplicationDestinationInfoResponse)

	if !ok {
		err := fmt.Errorf("received response of unexpected type")
		logger.Error(err, "unable to parse GetReplicationDestinationInfo response", "VRName", instance.Name)
		return nil, err
	}
	return destResp, nil
}

func getInfoReconcileInterval(parameters map[string]string, logger logr.Logger) time.Duration {
	rawScheduleTime := parameters["schedulingInterval"]
	if rawScheduleTime == "" {
		return defaultScheduleTime
	}
	scheduleTime, err := time.ParseDuration(rawScheduleTime)
	if err != nil {
		logger.Error(err, "failed to parse schedulingInterval, using default", "value", rawScheduleTime)
		return defaultScheduleTime
	}
	return scheduleTime
}

func protoReplicationStatusToString(status replicationlib.GetVolumeReplicationInfoResponse_Status) string {
	switch status {
	case replicationlib.GetVolumeReplicationInfoResponse_HEALTHY:
		return "Healthy"
	case replicationlib.GetVolumeReplicationInfoResponse_DEGRADED:
		return "Degraded"
	case replicationlib.GetVolumeReplicationInfoResponse_ERROR:
		return "Error"
	default:
		return "Unknown"
	}
}

func (r *VolumeReplicationReconciler) isParentVRGSecondary(ctx context.Context, instance *replicationv1alpha1.VolumeReplication, logger logr.Logger) bool {
	for _, ref := range instance.GetOwnerReferences() {
		if ref.Kind == "VolumeReplicationGroup" {
			vrg := &unstructured.Unstructured{}
			vrg.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "ramendr.openshift.io",
				Version: "v1alpha1",
				Kind:    "VolumeReplicationGroup",
			})
			if err := r.Get(ctx, types.NamespacedName{
				Name:      ref.Name,
				Namespace: instance.Namespace,
			}, vrg); err != nil {
				logger.Error(err, "failed to get parent VRG", "VRGName", ref.Name)
				return false
			}
			state, found, _ := unstructured.NestedString(vrg.Object, "spec", "replicationState")
			if found && replicationv1alpha1.ReplicationState(state) == replicationv1alpha1.Secondary {
				return true
			}
		}
	}
	return false
}
