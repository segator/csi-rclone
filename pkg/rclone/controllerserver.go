package rclone

import (
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
	"os"

	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	RcloneOps Operations
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "DeteleVolume must be provided volume id")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume without capabilities")
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.VolumeContext,
			VolumeCapabilities: req.VolumeCapabilities,
			Parameters:         req.Parameters,
		},
	}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ControllerPublishVolume not implemented")
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ControllerUnpublishVolume not implemented")
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	volumeName := req.GetName()
	if len(volumeName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume name must be provided")
	}

	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume without capabilities")
	}

	rcloneConfPath, err := extractRcloneConf(req.Secrets)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateVolume: %v", err)
	}
	remote, ok := req.GetParameters()["remote"]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "remote key not found in parameters")
	}
	remotePath, ok := req.GetParameters()["path"]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "path key not found in parameters")
	}

	if err = cs.RcloneOps.CreateVol(ctx, volumeName, remote, remotePath, rcloneConfPath); err != nil {
		klog.Errorf("error creating Volume: %s", err)
		return nil, err
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
			VolumeId:      uuid.New().String(),
			VolumeContext: map[string]string{
				"remote": remote,
				"path":   fmt.Sprintf("%s/%s", remotePath, volumeName),
			},
		},
	}, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "DeteleVolume must be provided volume id")
	}

	rcloneConfPath, err := extractRcloneConf(req.Secrets)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateVolume: %v", err)
	}

	rcloneVol, err := cs.RcloneOps.GetVolumeById(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = cs.RcloneOps.DeleteVol(ctx, rcloneVol, rcloneConfPath)
	if err != nil {
		klog.Errorf("error creating Volume: %s", err)
		return nil, err
	}

	return &csi.DeleteVolumeResponse{}, nil

}

func (*controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ControllerExpandVolume not implemented")
}

func (cs *controllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return &csi.ControllerGetVolumeResponse{Volume: &csi.Volume{
		VolumeId: req.VolumeId,
	}}, nil
}

func extractRcloneConf(secrets map[string]string) (string, error) {
	rcloneConfData, ok := secrets["rclone.conf"]
	if !ok {
		return "", fmt.Errorf("rclone.conf key in secret not found")
	}
	rcloneConf, err := os.CreateTemp("", "rclone.conf")
	if err != nil {
		return "", err
	}

	if _, err = rcloneConf.Write([]byte(rcloneConfData)); err != nil {
		return "", err
	}

	if err = rcloneConf.Close(); err != nil {
		return "", err
	}
	return rcloneConf.Name(), nil
}

// single operand routine.
