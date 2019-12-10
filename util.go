package os_brick

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func Execute(name string, arg ...string) (string, error) {
	cmd := exec.Command(name, arg...)
	stdoutStderr, err := cmd.CombinedOutput()
	return string(stdoutStderr), err
}

// ExecWithTimeout executes a timeouted command.
// The program path is defined by the name arguments, args are passed as arguments to the program.
//
// ExecWithTimeout returns process output as a string (stdout) , and stderr as an error.
func ExecWithTimeout(timeout time.Duration, name string, args ...string) (string, error) {
	c := exec.Command(name, args...)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	c.Stdout = stdout
	c.Stderr = stderr

	if err := c.Start(); err != nil {
		return "", err
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.Process.Wait()
		done <- err
	}()
	select {
	case <-time.After(timeout):
		_ = c.Process.Signal(os.Kill)
	case <-done:
	}

	res := string(stdout.Bytes())
	if err := string(stderr.Bytes()); len(err) > 0 {
		return res, errors.New(err)
	}
	return res, nil
}

func IsFileExists(file string) bool {
	if _, err := os.Stat(file); err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	} else {
		// TODO: Schrodinger: file may or may not exist. See err for details.
		//  Therefore, do *NOT* use !os.IsNotExist(err) to test for file existence
		return false
	}
}

func RunWithRetry(maxRetry int, interval time.Duration, exec func(currentTry int) bool) bool {
	tries := 1
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	//execute instantly
	if exec(tries) {
		return true
	}
	for {
		select {
		case <-ticker.C:
			if tries >= maxRetry {
				return false
			}
			if exec(tries) {
				return true
			}
			tries++
		}
	}
}

func CheckValidDevice(device string) bool {
	_, err := Execute("dd", "if="+device, "of=/dev/null", "count=1")
	if err != nil {
		log.Print("failed to access the device on the path ", device, err)
		return false
	}
	return true
}

func IsNumeric(s string) (bool, float64) {
	f, err := strconv.ParseFloat(s, 64)
	return err == nil, f
}

// MountDir
func MountDir(path, dir string, flag string) error {
	// mount -o rw /dev/dm-X /mnt/vdisk/X
	out, err := Execute("mount", "-o", flag, path, dir)
	if err != nil {
		return fmt.Errorf("execute mount -o %s %s to %s failed: %v", flag, path, dir, err)
	}
	log.Printf("execute mount -o %s %s to %s : %s", flag, path, dir, out)
	return nil
}

// Mkfs
func Mkfs(device, fsType string) error {
	// mkfs -t ext4 /dev/sdj
	out, err := Execute("mkfs", "-t", fsType, device)
	if err != nil {
		return fmt.Errorf("execute mkfs -t %s %s failed: %v", fsType, device, err)
	}
	log.Printf("execute mkfs -t %s %s : %s", fsType, device, out)
	return nil
}

// UnmountDir
func UnmountDir(dir string, rmDir bool) error {
	// umount /opt/kubelet/pods/xxx/volumes/xxx
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Printf("execute umount: dir %s seems no longer exists.", dir)
		return nil
	}

	out, err := ExecWithTimeout(time.Second*10, "umount", dir)
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			log.Printf("execute umount faild, file already deleted")
			return nil
		}
		return fmt.Errorf("execute umount failed: %v", err)
	}
	log.Printf("execute umount SUCCESS: %s", out)

	if rmDir {
		err = os.RemoveAll(dir)
		//out, err = internal.ExecLocal("rm", "-rf", dir)
		//if err != nil && err.Error() != ErrorNoResult {
		if err != nil {
			return fmt.Errorf("remove dir %s failed: %v", dir, err)
		}
		log.Printf("execute rm -rf %s : %s, %v", dir, out, err)
	}

	return nil
}
