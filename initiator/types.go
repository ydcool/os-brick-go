package initiator

type HBA map[string]string

type MultipathDevice map[string]string

//(pci_id,wwn,lun)
type Device []string

//(wwn,lun)
type Target []string
