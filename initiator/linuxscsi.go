/**
Generic linux Fibre Channel utilities

Inspired by github.com/openstack/os-brick

@author Dominic Yin <yindongchao@inspur.com>

*/
package initiator

import (
	"fmt"
	osBrick "github.com/ydcool/os-brick-go"
	"log"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

//RemoveSCSIDevice Removes a scsi device based upon /dev/sdX name.
func RemoveSCSIDevice(device string, flush bool) error {
	path := fmt.Sprintf("/sys/block/%s/device/delete", strings.Replace(device, "/dev/", "", 1))
	if osBrick.IsFileExists(path) {
		if flush {
			if err := FlushDeviceIO(device); err != nil {
				return err
			}
		}
		return EchoSCSICommand(path, "1")
	}
	return nil
}

//FlushDeviceIO This is used to flush any remaining IO in the buffers.
func FlushDeviceIO(device string) error {
	if osBrick.IsFileExists(device) {
		//NOTE(geguileo): With 30% connection error rates flush can get
		//stuck, set timeout to prevent it from hanging here forever.
		//Retry twice after 20 and 40 seconds.
		osBrick.RunWithRetry(3, time.Second*10, func(_ int) bool {
			out, err := osBrick.ExecWithTimeout(time.Minute*3, "blockdev", "--flushbufs", device)
			if err != nil {
				log.Printf("failed execute blockdev --flushbufs %s: %s, ERROR: %v", device, out, err)
				return false
			}
			log.Printf("execute blockdev --flushbufs %s: %s", device, out)
			return true
		})
	}
	return nil
}

//Read the WWN from page 0x83 value for a SCSI device.
func GetSCSIWWN(path string) (string, error) {
	out, err := osBrick.Execute("/lib/udev/scsi_id", "--page", "0x83", "--whitelisted", path)
	return strings.TrimSpace(out), err
}

//Look for the multipath device file for a volume WWN.
//
//	Multipath devices can show up in several places on
//	a linux system.
//
//	1) When multipath friendly names are ON:
//	    a device file will show up in
//	    /dev/disk/by-id/dm-uuid-mpath-<WWN>
//	    /dev/disk/by-id/dm-name-mpath<N>
//	    /dev/disk/by-id/scsi-mpath<N>
//	    /dev/mapper/mpath<N>
//
//	2) When multipath friendly names are OFF:
//	    /dev/disk/by-id/dm-uuid-mpath-<WWN>
//	    /dev/disk/by-id/scsi-<WWN>
//	    /dev/mapper/<WWN>
func FindMultipathDevicePath(deviceWwn string) (string, error) {
	//First look for the common path
	path := "/dev/disk/by-id/dm-uuid-mpath-" + deviceWwn
	if WaitForPath(path) {
		return path, nil
	}
	//for some reason the common path wasn't found
	//lets try the dev mapper path
	path = "/dev/mapper/" + deviceWwn
	if WaitForPath(path) {
		return path, nil
	}
	return "", fmt.Errorf("couldn't find a valid multipath device path for %s", deviceWwn)
}

//Discover multipath devices for a mpath device.
//
//	This uses the slow multipath -l command to find a
//	multipath device description, then screen scrapes
//	the output to discover the multipath device name
//	and it's devices.
func FindMultipathDevice(deviceName string) (map[string]interface{}, error) {
	var (
		mDev     string
		mDevID   string
		mDevName string
		devices  []MultipathDevice
		out      string
		err      error
	)
	out, err = osBrick.Execute("multipath", "-l", deviceName)
	if err != nil {
		return nil, err
	}
	if out != "" {
		lines := strings.Split(strings.TrimSpace(out), "\n")
		reg, err := regexp.Compile(MultipathErrorRegex)
		if err != nil {
			return nil, err
		}
		newLines := make([]string, 0)
		for _, l := range lines {
			if l != "" && !reg.MatchString(l) {
				newLines = append(newLines, l)
			}
		}
		if len(newLines) > 0 {
			ns := strings.Split(newLines[0], " ")
			mDevName = ns[0]
			if _, ok := MultipathDeviceActions[mDevName]; ok {
				mDevName = ns[1]
			}
			mDev = "/dev/mapper/" + mDevName

			//Confirm that the device is present.
			if !osBrick.IsFileExists(mDev) {
				return nil, fmt.Errorf("couldn't find multipath device %s", mDev)
			}

			reg, err = regexp.Compile(MultipathWWIDRegex)
			if err != nil {
				return nil, err
			}

			wwidSearch := reg.FindStringSubmatch(newLines[0])
			if len(wwidSearch) > 0 {
				mDevID = wwidSearch[1]
			} else {
				mDevID = mDevName
			}
			deviceLines := newLines[3:]
			for _, l := range deviceLines {
				if strings.Contains(l, "policy") {
					continue
				}
				devLine := strings.TrimLeft(l, " |-`")
				devInfo := strings.Split(devLine, " ")
				address := strings.Split(devInfo[0], ":")
				dev := MultipathDevice{
					"device":  "/dev/" + devInfo[1],
					"host":    address[0],
					"channel": address[1],
					"id":      address[2],
					"lun":     address[3],
				}
				devices = append(devices, dev)
			}
		}
	}

	if mDev != "" {
		info := map[string]interface{}{
			"device":  mDev,
			"id":      mDevID,
			"name":    mDevName,
			"devices": devices,
		}
		return info, nil
	}
	return nil, nil
}

//Wait for a path to show up.
func WaitForPath(path string) bool {
	if osBrick.IsFileExists(path) {
		return true
	}
	return osBrick.RunWithRetry(3, time.Second, func(_ int) bool {
		return osBrick.IsFileExists(path)
	})
}

//WaitForRW Wait for block device to be Read-Write.
func WaitForRW(deviceWwn string, devicePath string) error {
	log.Printf("checking to see if %s is read-only", devicePath)
	out, err := osBrick.Execute("lsblk", "-o", "NAME,RO", "-l", "-n")
	if err != nil {
		return err
	}
	blkdevs := strings.Split(out, "\n")
	for _, l := range blkdevs {
		//Entries might look like:
		//
		//   "3624a93709a738ed78583fd120013902b (dm-1)  1"
		//
		// or
		//
		//   "sdd                                       0"
		//
		// We are looking for the first and last part of them. For FC
		// multipath devices the name is in the format of '<WWN> (dm-<ID>)'
		blkdevParts := strings.Split(l, " ")
		ro := blkdevParts[len(blkdevParts)-1]
		name := blkdevParts[0]

		//We must validate that all pieces of the dm-# device are rw,
		//if some are still ro it can cause problems.
		roi, err := strconv.Atoi(ro)
		if err != nil {
			return err
		}
		if strings.Contains(name, deviceWwn) && roi == 1 {
			log.Printf("block device %s is read-only", devicePath)
			_, err := osBrick.Execute("multipath", "-r")
			return err
		}
	}
	log.Printf("Block device %s is not read-only.", devicePath)
	return nil
}

func ProcessLunID(lunIDs interface{}) (interface{}, error) {
	if ids, ok := lunIDs.([]interface{}); ok {
		processed := make([]interface{}, 0)
		for _, x := range ids {
			if xx, err := formatLunID(x); err == nil {
				processed = append(processed, xx)
			} else {
				return nil, err
			}
		}
		return processed, nil
	} else {
		if processed, err := formatLunID(lunIDs); err != nil {
			return nil, err
		} else {
			return processed, nil
		}
	}
}

func formatLunID(x interface{}) (interface{}, error) {
	//make sure lun_id is an int
	if lunID, ok := x.(int); ok {
		if lunID < 256 {
			return lunID, nil
		} else {
			return fmt.Sprintf("0x%04x%04x00000000", lunID&0xffff, lunID>>16&0xffff), nil
		}
	} else if s, ok := x.(string); ok {
		i, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("lun_id cannot convert to int: %s", s)
		}
		return i, nil
	}
	return nil, fmt.Errorf("lun_id should be int value: %#v", x)
}

//Used to echo strings to scsi subsystem.
func EchoSCSICommand(path, content string) error {
	//out, err := Execute("tee", "-a", path, content)
	cmd := fmt.Sprintf(`echo '%s' > %s`, content, path)
	_, err := osBrick.Execute("sh", "-c", cmd)
	return err
}

//Translates /dev/disk/by-path/ entry to /dev/sdX.
func GetNameFromPath(path string) string {
	name, err := filepath.EvalSymlinks(path)
	if err != nil {
		log.Printf("failed get realpath for path: %s, ERROR: %v", path, err)
		return ""
	}
	if strings.HasPrefix(name, "/dev/") {
		return name
	} else {
		return ""
	}
}

func FlushMultipathDevice(wwn string) {
	log.Printf("flush multipath device %s", wwn)
	//NOTE(geguileo): With 30% connection error rates flush can get stuck,
	//set timeout to prevent it from hanging here forever.  Retry twice
	//after 20 and 40 seconds.
	osBrick.RunWithRetry(3, time.Second*10, func(_ int) bool {
		out, err := osBrick.ExecWithTimeout(time.Minute*3, "multipath", "-f", wwn)
		log.Printf("exec multipath -f %s: %s", wwn, out)
		return err == nil
	})
}

func GetDeviceInfo(device string) (map[string]string, error) {
	out, err := osBrick.Execute("sg_scan", device)
	log.Printf("exec sg_scan %s: %s", device, out)
	if err != nil {
		return nil, fmt.Errorf("failed execute sg_scan %s: %v", device, err)
	}
	deviceInfo := map[string]string{
		"device":  device,
		"host":    "",
		"channel": "",
		"id":      "",
		"lun":     "",
	}
	if out != "" {
		line := strings.TrimSpace(out)
		line = strings.ReplaceAll(line, device+": ", "")
		info := strings.Split(line, " ")

		for _, item := range info {
			if strings.Contains(item, "=") {
				pair := strings.Split(item, "=")
				deviceInfo[pair[0]] = pair[1]
			} else if strings.Contains(item, "scsi") {
				deviceInfo["host"] = strings.ReplaceAll(item, "scsi", "")
			}
		}
	}
	return deviceInfo, nil
}

//Determine what path was used by Nova/Cinder to access volume
func GetDevPath(connProperties map[string]interface{}, deviceInfo map[string]string) string {
	if deviceInfo != nil {
		if path, ok := deviceInfo["path"]; ok {
			return path
		}
	}
	if devPath, ok := connProperties["device_path"]; ok {
		if path, ok := devPath.(string); ok {
			return path
		}
	}
	return ""
}

//Check if a device needs to be flushed when detaching.
//
//	A device representing a single path connection to a volume must only be
//	flushed if it has been used directly by Nova or Cinder to write data.
//
//	If the path has been used via a multipath DM or if the device was part
//	of a multipath but a different single path was used for I/O (instead of
//	the multipath) then we don't need to flush.
func RequiresFlush(devicePath string, pathUsed string, wasMultipath bool) (bool, error) {
	//No used path happens on failed attachs, when we don't care about individual flushes.
	//When Nova used a multipath we don't need to do individual flushes.
	if pathUsed == "" || wasMultipath {
		return false, nil
	}
	//We need to flush the single path that was used.
	//For encrypted volumes the symlink has been replaced, so realpath
	//won't return device under /dev but under /dev/disk/...
	rPath, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		return false, fmt.Errorf("failed get realpath for path:%s: %v", devicePath, err)
	}
	rPathUsed, err := filepath.EvalSymlinks(pathUsed)
	if err != nil {
		return false, fmt.Errorf("failed get realpath for path:%s: %v", pathUsed, err)
	}
	dir, _ := filepath.Split(rPathUsed)
	return rPathUsed == rPath || dir != "/dev", nil
}

//Signal the SCSI subsystem to test for volume resize.
//
//	This function tries to signal the local system's kernel
//	that an already attached volume might have been resized.
func DoExtendVolume(volumePaths []string, useMultipath bool) (float64, error) {
	log.Printf("extending volume %v", volumePaths)
	var newSize = 0.0
	for _, volumePath := range volumePaths {
		device, err := GetDeviceInfo(volumePath)
		if err != nil {
			log.Printf("failed get device info for path: %s, ERROR: %v", volumePath, err)
			continue
		}
		log.Printf("volume device info: %#v", device)
		deviceId := fmt.Sprintf("%s:%s:%s:%s", device["host"], device["channel"], device["id"], device["lun"])
		scsiPath := fmt.Sprintf("/sys/bus/scsi/drivers/sd/%s", deviceId)
		size, err := GetDeviceSize(volumePath)
		if err != nil {
			log.Printf("failed get device size for path: %s, ERROR: %v", volumePath, err)
			continue
		}
		log.Printf("starting size: %f", size)

		//now issue the device rescan
		err = EchoSCSICommand(scsiPath+"/rescan", "1")
		if err != nil {
			log.Printf("failed echo '1' > %s, ERROR: %s", scsiPath+"/rescan", err)
		}
		newSize, err = GetDeviceSize(volumePath)
		if err != nil {
			log.Printf("failed get device size for path: %s, ERROR: %s", volumePath, err)
			continue
		}
		log.Printf("volume size after scsi device rescan %f", newSize)
	}

	scsiWWN, err := GetSCSIWWN(volumePaths[0])
	if err != nil {
		return 0, fmt.Errorf("failed get scsi wwn for path: %s", volumePaths[0])
	}
	if useMultipath {
		mPathDevice, err := FindMultipathDevicePath(scsiWWN)
		if err != nil {
			return 0, fmt.Errorf("failed find multipath device path for wwn %s : %v", scsiWWN, err)
		}
		if mPathDevice != "" {
			//Force a reconfigure so that resize works
			if err = MultipathReConfigure(); err != nil {
				return 0, fmt.Errorf("failed reconfigure multipath: %v", err)
			}
			size, err := GetDeviceSize(mPathDevice)
			if err != nil {
				return 0, fmt.Errorf("failed get device size for path %s after reconfigure: ", mPathDevice)
			}
			log.Printf("mpath %s current size: %f", mPathDevice, size)
			result, err := MultipathResizeMap(scsiWWN)
			if err != nil {
				return 0, fmt.Errorf("failed multipath resize map: %v", err)
			}
			if strings.Contains(result, "fail") {
				return 0, fmt.Errorf("multipathd failed to update the size mapping of multipath device %s volume %v", scsiWWN, volumePaths)
			}
			if newSize, err = GetDeviceSize(mPathDevice); err != nil {
				return 0, fmt.Errorf("failed get device size for path %s after resize map: ", mPathDevice)
			}
		}
	}
	return newSize, nil
}

//Issue a multipath resize map on device.
//
//	This forces the multipath daemon to update it's
//	size information a particular multipath device.
func MultipathResizeMap(wwn string) (string, error) {
	return osBrick.Execute("multipathd", "resize", "map", wwn)
}

//Get the size in bytes of a volume
func GetDeviceSize(path string) (float64, error) {
	out, err := osBrick.Execute("blockdev", "--getsize64", path)
	if err != nil {
		return 0, fmt.Errorf("failed execute blockdev --getsize64 %s: %v", path, err)
	}
	s := strings.TrimSpace(out)
	if b, f := osBrick.IsNumeric(s); b {
		return f, nil
	}
	return 0, fmt.Errorf("device size not numeric: %s", s)
}

//Issue a multipathd reconfigure.
//
//	When attachments come and go, the multipathd seems
//	to get lost and not see the maps.  This causes
//	resize map to fail 100%.  To overcome this we have
//	to issue a reconfigure prior to resize map.
func MultipathReConfigure() error {
	out, err := osBrick.Execute("multipathd", "reconfigure")
	log.Printf("execute multipathd reconfigure: %s", out)
	return err
}
