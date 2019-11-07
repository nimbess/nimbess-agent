package util

import (
	"errors"
	"fmt"
	"github.com/jaypipes/ghw"
	"strings"
)

func ValidatePCI(addr string) error {
	pci, err := ghw.PCI()
	if err != nil {
		return err
	}

	deviceInfo := pci.GetDevice(addr)
	if deviceInfo == nil {
		return errors.New("invalid PCI address")
	}

	if !strings.Contains(deviceInfo.Class.Name, "Ethernet") && !strings.Contains(deviceInfo.Class.Name,
		"Network") {
		return fmt.Errorf("PCI device is not a valid network type: %s", deviceInfo.Class.Name)
	}

	return nil
}
