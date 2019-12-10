/**
Generic linux Fibre Channel utilities

Inspired by github.com/openstack/os-brick

@author Dominic Yin <yindongchao@inspur.com>

*/
package connectors

import (
	"fmt"
	osBrick "github.com/ydcool/os-brick-go"
	"github.com/ydcool/os-brick-go/initiator"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

//Connect to a volume.
//
//  The connection_properties describes the information needed by
//  the specific protocol to use to make the connection.
//
//  The connection_properties is a dictionary that describes the target
//  volume.  It varies slightly by protocol type (iscsi, fibre_channel),
//  but the structure is usually the same.
//
//
//  An example for iSCSI:
//
//  {'driver_volume_type': 'iscsi',
//   'data': {
//       'target_luns': [0, 2],
//       'target_iqns': ['iqn.2000-05.com.3pardata:20810002ac00383d',
//                       'iqn.2000-05.com.3pardata:21810002ac00383d'],
//       'target_discovered': True,
//       'encrypted': False,
//       'qos_specs': None,
//       'target_portals': ['10.52.1.11:3260', '10.52.2.11:3260'],
//       'access_mode': 'rw',
//  }}
//
//  An example for fibre_channel with single lun:
//
//  {'driver_volume_type': 'fibre_channel',
//   'data': {
//      'initiator_target_map': {'100010604b010459': ['20210002AC00383D'],
//                               '100010604b01045d': ['20220002AC00383D']},
//      'target_discovered': True,
//      'encrypted': False,
//      'qos_specs': None,
//      'target_lun': 1,
//      'access_mode': 'rw',
//      'target_wwn': [
//          '20210002AC00383D',
//          '20220002AC00383D',
//          ],
//   }}
//
//  An example for fibre_channel target_wwns and with different LUNs and
//  all host ports mapped to target ports:
//
//  {'driver_volume_type': 'fibre_channel',
//   'data': {
//      'initiator_target_map': {
//          '100010604b010459': ['20210002AC00383D', '20220002AC00383D'],
//          '100010604b01045d': ['20210002AC00383D', '20220002AC00383D']
//          },
//      'target_discovered': True,
//      'encrypted': False,
//      'qos_specs': None,
//      'target_luns': [1, 2],
//      'access_mode': 'rw',
//      'target_wwns': ['20210002AC00383D', '20220002AC00383D'],
//   }}
//
//   For FC the dictionary could also present the enable_wildcard_scan key
//   with a boolean value (defaults to True) in case a driver doesn't want
//   OS-Brick to use a SCSI scan with wildcards when the FC initiator on
//   the host doesn't find any target port.
//
//   This is useful for drivers that know that sysfs gets populated
//   whenever there's a connection between the host's HBA and the storage
//   array's target ports.
//
//  :param connection_properties: The dictionary that describes all
//                                of the target volume attributes.
//  :type connection_properties: dict
//  :returns: map[string]string{"path":"/dev/disk/by-path/pci-0000:08:00.0-fc-0x2100001b32808c84-lun-1", "scsi_wwn":"23265626235666332", "type":"block"}
func ConnectVolume(connectionProperties map[string]interface{}) (map[string]string, error) {
	deviceInfo := map[string]string{
		"type": "block",
	}
	connProperties, err := addTargetsToConnectionProperties(connectionProperties)
	if err != nil {
		return nil, err
	}
	log.Printf("add Targets To connProps: %#v", connProperties)
	hbas, err := initiator.GetFCHBAsInfo()
	log.Printf("FC HBAs Info: %#v", hbas)
	if err != nil {
		return nil, err
	}
	if len(hbas) == 0 {
		return nil, fmt.Errorf("we are unable to locate any Fibre Channel devices")
	}
	hostDevices, err := getPossibleVolumePaths(connProperties["targets"].([]initiator.Target), hbas)
	if err != nil {
		return nil, err
	}
	log.Printf("possibleVolumePaths: %#v", hostDevices)

	var hostDevice, deviceName string
	if !osBrick.RunWithRetry(initiator.DeviceScanAttemptsDefault, time.Second*5, func(_ int) bool {
		for _, dev := range hostDevices {
			if osBrick.IsFileExists(dev) && osBrick.CheckValidDevice(dev) {
				//get the /dev/sdX device. This is used to find the multipath device.
				hostDevice = dev
				deviceName, _ = filepath.EvalSymlinks(dev)
				return true
			}
		}
		initiator.RescanHosts(hbas, connProperties)
		return false
	}) {
		return nil, fmt.Errorf("fibre Channel volume device not found")
	}

	//find out the WWN of the device
	deviceWwn, err := initiator.GetSCSIWWN(hostDevice)
	if err != nil {
		return nil, err
	}
	deviceInfo["scsi_wwn"] = deviceWwn
	//see if the new drive is part of a multipath device.  If so, we'll use the multipath device.
	var (
		devicePath   string
		useMultipath = true
	)
	if um, ok := connProperties["use_multipath"]; ok {
		if umb, ok := um.(bool); ok {
			useMultipath = umb
		}
	}
	if useMultipath {
		var multipathId string
		devicePath, multipathId, err = discoverMPathDevice(deviceWwn, connProperties, deviceName)
		if err != nil {
			return nil, err
		}
		if multipathId != "" {
			// only set the multipath_id if we found one
			deviceInfo["multipath_id"] = multipathId
		}
	} else {
		devicePath = hostDevice
	}
	deviceInfo["path"] = devicePath
	return deviceInfo, nil
}

//Detach the volume from instance_name.
//
//	:param connection_properties: The dictionary that describes all
//	                              of the target volume attributes.
//	:type connection_properties: dict
//	:param device_info: historical difference, but same as connection_props
//	:type device_info: dict
//
//	connection_properties for Fibre Channel must include:
//	target_wwn - World Wide Name
//	target_lun - LUN id of the volume
func DisconnectVolume(connectionProperties map[string]interface{}, deviceInfo map[string]string) error {
	useMultipath := true
	if um, ok := connectionProperties["use_multipath"]; ok {
		if umb, ok := um.(bool); ok {
			useMultipath = umb
		}
	}
	devices := make([]map[string]string, 0)
	connProperties, err := addTargetsToConnectionProperties(connectionProperties)
	if err != nil {
		log.Printf("failed addTargetsToConnectionProperties: %#v, ERROR:%v", connectionProperties, err)
	}
	volumePaths, err := GetVolumePaths(connProperties["targets"].([]initiator.Target))
	if err != nil {
		return fmt.Errorf("failed get volume paths: %v", err)
	}
	log.Printf("get volume paths: %#v", volumePaths)
	mPathPath := ""
	for _, path := range volumePaths {
		realPath := initiator.GetNameFromPath(path)
		if useMultipath && mPathPath != "" && osBrick.CheckValidDevice(path) {
			wwn, err := initiator.GetSCSIWWN(path)
			if err != nil {
				log.Printf("failed get scsi wwn for path %s, ERROR:%v", path, err)
				continue
			}
			mPathPath, err = initiator.FindMultipathDevicePath(wwn)
			if err != nil {
				log.Printf("failed find multipath device path for wwn: %s, ERROR:%v", wwn, err)
				continue
			}
			if mPathPath != "" {
				initiator.FlushMultipathDevice(mPathPath)
			}
		}
		deviceInfo, err := initiator.GetDeviceInfo(realPath)
		if err != nil {
			log.Printf("failed get device info for path: %s, ERROR:%v", realPath, err)
			continue
		}
		devices = append(devices, deviceInfo)
	}

	if len(devices) == 0 {
		return fmt.Errorf("no device to remove")
	}
	log.Printf("devices to remove = %#v", devices)
	err = removeDevices(connProperties, devices, deviceInfo)
	if err != nil {
		return fmt.Errorf("failed remove devices %#v: %v", devices, err)
	}
	log.Print("devices removed successfully")
	return nil
}

//Update the local kernel's size information.
//
//	Try and update the local kernel's size information for an FC volume.
func ExtendVolume(connectionProperties map[string]interface{}) error {
	useMultipath := true
	if um, ok := connectionProperties["use_multipath"]; ok {
		if umb, ok := um.(bool); ok {
			useMultipath = umb
		}
	}
	connProperties, err := addTargetsToConnectionProperties(connectionProperties)
	if err != nil {
		return fmt.Errorf("failed add targets to connection properties:%v", err)
	}
	volumePaths, err := GetVolumePaths(connProperties["targets"].([]initiator.Target))
	if err != nil {
		return fmt.Errorf("failed get volume paths: %v", err)
	}
	if len(volumePaths) == 0 {
		return fmt.Errorf("couldn't find any volume paths on the host to extend volume for %#v", connProperties)
	}
	if newSize, err := initiator.DoExtendVolume(volumePaths, useMultipath); err != nil {
		return err
	} else {
		log.Print("volume extended to new size: ", newSize)
	}
	return nil
}

func GetVolumePaths(targets []initiator.Target) ([]string, error) {
	//first fetch all of the potential paths that might exist
	//how the FC fabric is zoned may alter the actual list
	//that shows up on the system.  So, we verify each path.
	volumePaths := make([]string, 0)
	hbas, err := initiator.GetFCHBAsInfo()
	if err != nil {
		return volumePaths, fmt.Errorf("failed get fc HBAs info: %v", err)
	}
	devicePaths, err := getPossibleVolumePaths(targets, hbas)
	if err != nil {
		return volumePaths, fmt.Errorf("failed get possible volume paths: %v", err)
	}
	for _, path := range devicePaths {
		if osBrick.IsFileExists(path) {
			volumePaths = append(volumePaths, path)
		}
	}
	return volumePaths, nil
}

//There may have been more than 1 device mounted
//by the kernel for this volume.  We have to remove all of them
func removeDevices(connProperties map[string]interface{}, devices []map[string]string, deviceInfo map[string]string) error {
	pathUsed := initiator.GetDevPath(connProperties, deviceInfo)
	wasMultipath := !strings.Contains(pathUsed, "/pci-")
	for _, device := range devices {
		devicePath := device["device"]
		flush, err := initiator.RequiresFlush(devicePath, pathUsed, wasMultipath)
		if err != nil {
			return fmt.Errorf("failed requires flush: devicePath:%s, pathUsed:%s, wasMultipath:%t", devicePath, pathUsed, wasMultipath)
		}
		if err = initiator.RemoveSCSIDevice(devicePath, flush); err != nil {
			return fmt.Errorf("failed remove scsi device: devicePath:%s, flush:%t", devicePath, flush)
		}
	}
	return nil
}

func getPossibleVolumePaths(targets []initiator.Target, hbas []initiator.HBA) ([]string, error) {
	possibleDevs := getPossibleDevices(hbas, targets)
	hostPaths, err := getHostDevices(possibleDevs)
	if err != nil {
		return nil, err
	}
	return hostPaths, nil
}

//Compute the possible fibre channel device options.
//	:param hbas: available hba devices.
//	:param targets: tuple of possible wwn addresses and lun combinations.
//
//	:returns: list of (pci_id, wwn, lun) tuples
//
//	Given one or more wwn (mac addresses for fibre channel) ports
//	do the matrix math to figure out a set of pci device, wwn
//	tuples that are potentially valid (they won't all be). This
//	provides a search space for the device connection.
func getPossibleDevices(hbas []initiator.HBA, targets []initiator.Target) []initiator.Device {
	rawDevices := make([]initiator.Device, 0)
	for _, hba := range hbas {
		if pciNum := getPCINum(hba); pciNum != "" {
			for _, t := range targets {
				targetWwn := fmt.Sprintf("0x%s", strings.ToLower(t[0]))
				rawDevices = append(rawDevices, initiator.Device{pciNum, targetWwn, t[1]})
			}
		}
	}
	return rawDevices
}

//NOTE(walter-boring)
//device path is in format of (FC and FCoE) :
///sys/devices/pci0000:00/0000:00:03.0/0000:05:00.3/host2/fc_host/host2
///sys/devices/pci0000:20/0000:20:03.0/0000:21:00.2/net/ens2f2/ctlr_2
///host3/fc_host/host3
//we always want the value prior to the host or net value
func getPCINum(hba initiator.HBA) string {
	if hba != nil {
		if devicePath, ok := hba["device_path"]; ok {
			devicePaths := strings.Split(devicePath, "/")
			for i, v := range devicePaths {
				if strings.HasPrefix(v, "net") || strings.HasPrefix(v, "host") {
					return devicePaths[i-1]
				}
			}
		}
	}
	return ""
}

//Compute the device paths on the system with an id, wwn, and lun
//	param : possibleDevs: list of (pci_id, wwn, lun) slices
//	return: list of device paths on the system based on the possibleDevs
func getHostDevices(possibleDevs []initiator.Device) ([]string, error) {
	prefix := ""
	hostDevices := make([]string, 0)
	for _, d := range possibleDevs {
		if lunID, err := initiator.ProcessLunID(d[2]); err != nil {
			return nil, err
		} else {
			hostDevice := fmt.Sprintf("/dev/disk/by-path/%spci-%s-fc-%s-lun-%v", prefix, d[0], d[1], lunID)
			rp, err := filepath.EvalSymlinks(hostDevice)
			if err != nil || !osBrick.IsFileExists(rp) {
				//on kylinos / arm64, host device has a special prefix:
				// /dev/disk/by-path/platform-40000000.pcie-controller-pci-0000:01:00.1-fc-0x2101001b32a08c84-lun-0
				log.Printf("host device %s with default prefix is not exists, we'll try to find it out", hostDevice)
				prefix, err = getPossibleHostPathPrefix()
				if err != nil {
					log.Printf("cannot found possible host device for %v under path /dev/disk/by-path/, ERROR: %v", d, err)
					continue
				}
				hostDevice = fmt.Sprintf("/dev/disk/by-path/%spci-%s-fc-%s-lun-%v", prefix, d[0], d[1], lunID)
			}
			hostDevices = append(hostDevices, hostDevice)
		}
	}
	return hostDevices, nil
}

//Where do we look for FC based volumes
func getPossibleHostPathPrefix() (string, error) {
	searchPath := "/dev/disk/by-path"
	reg, err := regexp.Compile(`(.*)pci-[a-z0-9]{4}:[a-z0-9]{2}:[a-z0-9]{2}.[a-z0-9]+-fc-0x[a-z0-9]{16}-lun-[a-z0-9]+`)
	if err != nil {
		return "", fmt.Errorf("failed compile regex: %v", err)
	}
	dir, err := os.Open(searchPath)
	if err != nil {
		return "", fmt.Errorf("failed read dir %s: %v", searchPath, err)
	}
	paths, err := dir.Readdirnames(-1)
	if err != nil {
		return "", fmt.Errorf("failed read dirnames for dir %s: %v", searchPath, err)
	}
	for _, p := range paths {
		matches := reg.FindStringSubmatch(p)
		log.Printf("possible host path and prefix: %#v", matches)
		if len(matches) > 1 {
			return matches[1], nil
		}
	}
	return "", fmt.Errorf("no matched path found under search path:%s", searchPath)
}

func addTargetsToConnectionProperties(connectionProperties map[string]interface{}) (map[string]interface{}, error) {
	var wwns []string
	targetWwn := connectionProperties["target_wwn"]
	targetWwns := connectionProperties["target_wwns"]
	if targetWwns != nil {
		wwns = targetWwns.([]string)
	} else {
		switch targetWwn.(type) {
		case []string:
			wwns = targetWwn.([]string)
		case string:
			wwns = []string{targetWwn.(string)}
		default:
			wwns = make([]string, 0)
		}
	}
	//Convert wwns to lower case
	lowWwns := make([]string, 0)
	for _, v := range wwns {
		vv := strings.ToLower(v)
		lowWwns = append(lowWwns, vv)
	}
	wwns = lowWwns
	if targetWwns != nil {
		connectionProperties["target_wwns"] = wwns
	} else if targetWwn != nil {
		connectionProperties["target_wwn"] = wwns
	}

	var luns []string
	targetLun := connectionProperties["target_lun"]
	targetLuns := connectionProperties["target_luns"]
	if targetLuns != nil {
		luns = targetLuns.([]string)
	} else if _, ok := targetLun.(string); ok {
		luns = []string{targetLun.(string)}
	} else {
		luns = make([]string, 0)
	}

	var targets []initiator.Target
	if len(luns) == len(wwns) && len(luns) > 0 {
		//Handles single wwwn + lun or multiple, potentially
		//different wwns or luns
		//targets = list(zip(wwns, luns))
		for i, w := range wwns {
			targets = append(targets, initiator.Target{w, luns[i]})
		}
	} else if len(luns) == 1 && len(wwns) > 1 {
		//For the case of multiple wwns, but a single lun (old path)
		targets = make([]initiator.Target, 0)
		for _, w := range wwns {
			targets = append(targets, initiator.Target{w, luns[0]})
		}
	} else {
		//Something is wrong, this shouldn't happen.
		return nil, fmt.Errorf("unable to find potential volume paths for FC device with luns %#v and wwns %#v", luns, wwns)
	}

	connectionProperties["targets"] = targets
	wwpnLunMap := make(map[string]string)
	for _, t := range targets {
		wwpnLunMap[t[0]] = t[1]
	}
	//If there is an initiator_target_map we can update it too
	if itMap, ok := connectionProperties["initiator_target_map"]; ok {
		//Convert it to lower
		//itmap = {k.lower(): [port.lower() for port in v] for k, v in itmap.items()}
		if itMap, ok := itMap.(map[string][]string); ok {
			lowItMap := make(map[string][]string)
			for k, v := range itMap {
				for _, port := range v {
					lowItMap[strings.ToLower(k)] = append(lowItMap[strings.ToLower(k)], strings.ToLower(port))
				}
			}
			connectionProperties["initiator_target_map"] = lowItMap

			newItMap := make(map[string][]string)
			for initWwpn, targetWwpns := range lowItMap {
				initTargets := make([]string, 0)
				for _, targetWwpn := range targetWwpns {
					if w, ok := wwpnLunMap[targetWwpn]; ok {
						initTargets = append(initTargets, w)
					}
				}
				newItMap[initWwpn] = initTargets
			}
			connectionProperties["initiator_target_lun_map"] = newItMap
		}
	}
	return connectionProperties, nil
}
