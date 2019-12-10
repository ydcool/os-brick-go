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
	"strings"
)

const (
	FCHostSysFSPath           = "/sys/class/fc_host"
	DeviceScanAttemptsDefault = 3
	MultipathErrorRegex       = `\w{3} \d+ \d\d:\d\d:\d\d \|.*$`
	MultipathPathCheckRegex   = `\s+\d+:\d+:\d+:\d+\s+`
	MultipathWWIDRegex        = `\((?P<wwid>.+)\)`
)

var (
	MultipathDeviceActions = map[string]bool{
		"unchanged:": false, "reject:": false, "reload:": false,
		"switchpg:": false, "rename:": false, "create:": false, "resize:": false}
)

func HasFCSupport() bool {
	return osBrick.IsFileExists(FCHostSysFSPath)
}

//GetFCHBAsInfo Get Fibre Channel WWNs and device paths from the system, if any.
//	Note(walter-boring) modern Linux kernels contain the FC HBA's in /sys
//	and are obtainable via the systool app
func GetFCHBAsInfo() ([]HBA, error) {
	hbas, err := GetFCHBAs()
	if err != nil {
		return nil, err
	}
	hbasInfo := make([]HBA, 0)
	for _, hba := range hbas {
		wwpn := strings.Replace(hba["port_name"], "0x", "", 1)
		wwnn := strings.Replace(hba["node_name"], "0x", "", 1)
		devicePath := hba["ClassDevicepath"]
		device := hba["ClassDevice"]
		hbasInfo = append(hbasInfo, HBA{
			"port_name":   wwpn,
			"node_name":   wwnn,
			"host_device": device,
			"device_path": devicePath,
		})
	}
	return hbasInfo, nil
}

//GetFCHBAs Get the Fibre Channel HBA information.
//
func GetFCHBAs() ([]HBA, error) {
	if !HasFCSupport() {
		//there is no FC support in the kernel loaded
		//so there is no need to even try to run systool
		return nil, fmt.Errorf("fc not supported")
	}
	out, err := osBrick.Execute("systool", "-c", "fc_host", "-v")
	if err != nil {
		return nil, err
	}
	hbas := make([]HBA, 0)
	if out == "" {
		return hbas, nil
	}
	lines := strings.Split(out, "\n")[2:]
	lastLine := ""
	hba := HBA{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		//2 newlines denotes a new hba port
		if line == "" && lastLine == "" {
			if len(hba) > 0 {
				hbas = append(hbas, hba)
				hba = HBA{}
			}
		} else {
			val := strings.Split(line, "=")
			if len(val) == 2 {
				key := strings.ReplaceAll(strings.TrimSpace(val[0]), " ", "")
				value := strings.TrimSpace(val[1])
				hba[key] = strings.ReplaceAll(value, `"`, "")
			}
		}
		lastLine = line
	}
	return hbas, nil
}

func RescanHosts(hbas []HBA, connProperties map[string]interface{}) {
	log.Printf("rescaning HBAs %v with connection properties %#v", hbas, connProperties)
	// Use initiator_target_lun_map (generated from initiator_target_map by
	// the FC connector) as HBA exclusion map
	var newHBAs = make([]HBA, 0)
	if ports, ok := connProperties["initiator_target_lun_map"]; ok {
		if portsMap, ok := ports.(map[string]interface{}); ok {
			for _, hba := range hbas {
				for k := range portsMap {
					if k == hba["port_name"] {
						newHBAs = append(newHBAs, hba)
					}
				}
			}
		}
		log.Printf("using initiator target map to exclude HBAs: %v", newHBAs)
	}

	//Most storage arrays get their target ports automatically detected
	//by the Linux FC initiator and sysfs gets populated with that
	//information, but there are some that don't.  We'll do a narrow scan
	//using the channel, target, and LUN for the former and a wider scan
	//for the latter.  If all paths to a former type of array were down on
	//the system boot the array could look like it's of the latter type
	//and make us bring us unwanted volumes into the system by doing a
	//broad scan.  To prevent this from happening Cinder drivers can use
	//the "enable_wildcard_scan" key in the connection_info to let us know
	//they don't want us to do broad scans even in those cases.
	broadScan := true
	if ews, ok := connProperties["enable_wildcard_scan"]; ok {
		broadScan = ews.(bool)
	}

	process := make([]interface{}, 0)
	skipped := make([]interface{}, 0)

	for _, hba := range hbas {
		ctls, lunsWildcards := getHBAChannelSCSITargetLun(hba, connProperties)
		//If we found the target ports, ignore HBAs that din't find them
		if len(ctls) > 0 {
			process = append(process, []interface{}{hba, ctls})
		} else if !broadScan {
			//If target ports not found and should have, then the HBA is not
			//connected to our storage
			log.Printf("skipping HBA %s, nothing to scan, target port not connected to initiator", hba["node_name"])
		} else if len(process) == 0 {
			//skipped.append((hba,[('-', '-', lun) for lun in luns_wildcards]))
			luns := make([]interface{}, 0)
			for k := range lunsWildcards {
				luns = append(luns, []string{"-", "-", k})
			}
			skipped = append(skipped, []interface{}{hba, luns})
		}
		//If we didn't find any target ports use wildcards if they are enabled
		if len(process) == 0 {
			process = skipped
		}
		for _, p := range process {
			hba := p.([]interface{})[0].(HBA)
			ctls := p.([]interface{})[1]
			if ctlsStrs, ok := ctls.([][]string); ok {
				for _, c := range ctlsStrs {
					hbaChannel, targetId, targetLun := c[0], c[1], c[2]
					log.Printf("scanning host:%v, wwnn:%s, c:%v, t:%v, l:%v", hba["host_device"], hba["node_name"], hbaChannel, targetId, targetLun)
					err := EchoSCSICommand(fmt.Sprintf("/sys/class/scsi_host/%s/scan", hba["host_device"]),
						fmt.Sprintf("%v %v %v", hbaChannel, targetId, targetLun))
					if err != nil {
						log.Printf("failed scan scsi device: %v", err)
					}
				}
			} else if cltsIntfs, ok := ctls.([]interface{}); ok {
				for _, c := range cltsIntfs {
					cc, ok := c.([]string)
					if !ok {
						log.Printf("expect ctls is []string but not, %#v", c)
						continue
					}
					hbaChannel, targetId, targetLun := cc[0], cc[1], cc[2]
					log.Printf("scanning host:%v, wwnn:%s, c:%v, t:%v, l:%v", hba["host_device"], hba["node_name"], hbaChannel, targetId, targetLun)
					err := EchoSCSICommand(fmt.Sprintf("/sys/class/scsi_host/%s/scan", hba["host_device"]),
						fmt.Sprintf("%v %v %v", hbaChannel, targetId, targetLun))
					if err != nil {
						log.Printf("failed scan scsi device: %v", err)
					}
				}
			} else {
				log.Printf("expect ctls be [][]string or []interface{} but not: %#v", ctls)
			}
		}
	}
}

//Get Fibre Channel WWPNs from the system, if any.
func GetFCWWPNs() ([]string, error) {
	hbas, err := GetFCHBAs()
	if err != nil {
		return nil, err
	}
	wwpns := make([]string, 0)
	for _, hba := range hbas {
		if ol, ok := hba["port_state"]; ok && ol == "Online" {
			if wwpn, ok := hba["port_name"]; ok {
				wwpns = append(wwpns, strings.ReplaceAll(wwpn, "0x", ""))
			}
		}
	}
	return wwpns, nil
}

//Get Fibre Channel WWNNs from the system, if any.
func GetFCWWNNS() ([]string, error) {
	//Note(walter-boring) modern Linux kernels contain the FC HBA's in /sys
	//and are obtainable via the systool app
	hbas, err := GetFCHBAs()
	if err != nil {
		return nil, err
	}
	wwpns := make([]string, 0)
	for _, hba := range hbas {
		if ol, ok := hba["port_state"]; ok && ol == "Online" {
			if wwpn, ok := hba["node_name"]; ok {
				wwpns = append(wwpns, strings.ReplaceAll(wwpn, "0x", ""))
			}
		}
	}
	return wwpns, nil
}

//Get HBA channels, SCSI targets, LUNs to FC targets for given HBA.
//
//   Given an HBA and the connection properties we look for the HBA channels
//   and SCSI targets for each of the FC targets that this HBA has been
//   granted permission to connect.
//
//   For drivers that don't return an initiator to target map we try to find
//   the info for all the target ports.
//
//   For drivers that return an initiator_target_map we use the
//   initiator_target_lun_map entry that was generated by the FC connector
//   based on the contents of the connection information data to know which
//   target ports to look for.
//
//   :returns: 2-Tuple with the first entry being a list of [c, t, l]
//   entries where the target port was found, and the second entry of the
//   tuple being a set of luns for ports that were not found.
func getHBAChannelSCSITargetLun(hba HBA, connectionProperties map[string]interface{}) ([][]string, map[string]bool) {
	//We want the targets' WWPNs, so we use the initiator_target_map if
	//present for this hba or default to targets if not present.
	log.Printf("getHBAChannelSCSITargetLun: HBA: %#v, connProp: %#v", hba, connectionProperties)

	targets := connectionProperties["targets"].([]Target)

	if _, ok := connectionProperties["initiator_target_map"]; ok {
		//This map we try to use was generated by the FC connector
		if lunMap, ok := connectionProperties["initiator_target_lun_map"]; ok {
			if lm, ok := lunMap.(map[string]interface{}); ok {
				if k, ok := lm[hba["port_name"]]; ok {
					targets = k.([]Target)
				}
			}
		}
	}
	//Leave only the number from the host_device field (ie: host6)
	hostDevice, ok := hba["host_device"]
	if ok && len(hostDevice) > 4 {
		hostDevice = hostDevice[4:]
	}

	path := fmt.Sprintf("/sys/class/fc_transport/target%s:", hostDevice)
	ctls := make([][]string, 0)
	lunNotFound := make(map[string]bool) //use map as set
	for _, t := range targets {
		wwpn, lun := t[0], t[1]
		//cmd = 'grep -Gil "%(wwpns)s" %(path)s*/port_name' % {'wwpns': wwpn,'path': path}
		cmd := fmt.Sprintf(`grep -Gil "%s" %s*/port_name`, wwpn, path)
		out, err := osBrick.Execute("sh", "-c", cmd)
		if err != nil {
			log.Printf("could not get HBA channel and SCSI target ID, path: %s, resaon:%v", path, err)
			//If we didn't find any paths add it to the not found list
			lunNotFound[fmt.Sprintf("%v", lun)] = true
		}
		//ctls += [  line.split('/')[4].split(':')[1:] + [lun] for line in out.split('\n') if line.startswith(path)]
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, path) {
				c := append(append([]string{}, strings.Split(strings.Split(line, "/")[4], ":")[1:]...), lun)
				ctls = append(ctls, c)
			}
		}
	}
	return ctls, lunNotFound
}
