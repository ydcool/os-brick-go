package initiator

import "testing"

func TestGetFCHBAs(t *testing.T) {
	hbas, err := GetFCHBAs()
	if err != nil {
		t.Error(err)
	}
	t.Log(hbas)
}
