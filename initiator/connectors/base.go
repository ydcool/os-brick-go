package connectors

import (
	osBrick "github.com/ydcool/os-brick-go"
	"github.com/ydcool/os-brick-go/initiator"
	"log"
	"path/filepath"
	"time"
)

//This method discovers a multipath device.
//
//	Discover a multipath device based on a defined connection_property
//	and a device_wwn and return the multipath_id and path of the multipath
//	enabled device if there is one.
func discoverMPathDevice(deviceWwn string, connProperties map[string]interface{}, deviceName string) (string, string, error) {
	path, err := initiator.FindMultipathDevicePath(deviceWwn)
	if err != nil {
		return "", "", err
	}
	var (
		devicePath, multipathID string
	)
	if path == "" {
		//find_multipath_device only accept realpath not symbolic path
		deviceRealPath, err := filepath.EvalSymlinks(deviceName)
		if err != nil {
			return "", "", err
		}
		mPathInfo, err := initiator.FindMultipathDevice(deviceRealPath)
		if mPathInfo != nil && err == nil {
			devicePath = mPathInfo["device"].(string)
			multipathID = deviceWwn
		} else {
			//we didn't find a multipath device.
			//so we assume the kernel only sees 1 device
			devicePath = deviceName
		}
	} else {
		devicePath = path
		multipathID = deviceWwn
	}
	if am, ok := connProperties["access_mode"]; ok && am != "ro" {
		//Sometimes the multipath devices will show up as read only
		//initially and need additional time/rescans to get to RW.
		success := osBrick.RunWithRetry(5, time.Second, func(_ int) bool {
			err := initiator.WaitForRW(deviceWwn, devicePath)
			return err == nil
		})
		if !success {
			log.Printf("block device %s is still read-only. Continuing anyway.", devicePath)
		}
	}
	return devicePath, multipathID, nil
}
