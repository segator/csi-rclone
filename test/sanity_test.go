package test

import (
	"github.com/kubernetes-csi/csi-test/pkg/sanity"
	"github.com/wunderio/csi-rclone/pkg/rclone"
	"io/ioutil"
	"os"
	"testing"
)

func TestMyDriver(t *testing.T) {
	// Setup the full driver and its environment
	endpoint := "unix:///plugin/csi.sock"
	driver := rclone.NewDriver("hostname", endpoint)
	go driver.Run()

	mntDir, err := ioutil.TempDir("", "mnt")
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(mntDir)
	//defer os.RemoveAll(mntDir)

	mntStageDir, err := ioutil.TempDir("", "mnt-stage")
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(mntStageDir)
	//defer os.RemoveAll(mntStageDir)
	cfg := &sanity.Config{

		StagingPath: mntStageDir,
		TargetPath:  mntDir,
		Address:     endpoint,
	}
	sanity.Test(t, cfg)
}
