// Copyright © 2015-2016 Platina Systems, Inc. All rights reserved.
// Use of this source code is governed by the GPL-2 license described in the
// LICENSE file.

package main

import (
	"fmt"
	"os/exec"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/platinasystems/go/eeprom"
	"github.com/platinasystems/go/goes"
	"github.com/platinasystems/go/goes/builtin"
	"github.com/platinasystems/go/goes/builtin/license"
	"github.com/platinasystems/go/goes/builtin/patents"
	"github.com/platinasystems/go/goes/core"
	"github.com/platinasystems/go/goes/fs"
	"github.com/platinasystems/go/goes/kernel"
	"github.com/platinasystems/go/goes/machine"
	"github.com/platinasystems/go/goes/machine/start"
	"github.com/platinasystems/go/goes/machine/stop"
	"github.com/platinasystems/go/goes/net"
	"github.com/platinasystems/go/goes/net/nld"
	"github.com/platinasystems/go/goes/net/vnet"
	"github.com/platinasystems/go/goes/net/vnetd"
	"github.com/platinasystems/go/goes/redis"
	"github.com/platinasystems/go/goes/sockfile"
	"github.com/platinasystems/go/goes/test"
	goredis "github.com/platinasystems/go/redis"
	govnet "github.com/platinasystems/go/vnet"
	"github.com/platinasystems/go/vnet/devices/ethernet/ixge"
	"github.com/platinasystems/go/vnet/devices/ethernet/switch/fe1"
	fe1copyright "github.com/platinasystems/go/vnet/devices/ethernet/switch/fe1/copyright"
	"github.com/platinasystems/go/vnet/ethernet"
	"github.com/platinasystems/go/vnet/ip4"
	"github.com/platinasystems/go/vnet/ip6"
	"github.com/platinasystems/go/vnet/pg"
	"github.com/platinasystems/go/vnet/unix"
)

const UsrShareGoes = "/usr/share/goes"

func main() {
	const fe1path = "github.com/platinasystems/go/vnet/devices/ethernet/switch/fe1"
	license.Others = []license.Other{{fe1path, fe1copyright.License}}
	patents.Others = []patents.Other{{fe1path, fe1copyright.Patents}}
	g := make(goes.ByName)
	g.Plot(builtin.New()...)
	g.Plot(core.New()...)
	g.Plot(fs.New()...)
	g.Plot(kernel.New()...)
	g.Plot(machine.New()...)
	g.Plot(net.New()...)
	g.Plot(redis.New()...)
	// g.Plot(test.New()...)
	_ = test.New
	g.Plot(vnet.New(), vnetd.New())
	start.Machine = "platina-mk1"
	start.RedisDevs = []string{"lo", "eth0"}
	start.ConfHook = wait4vnet
	start.PubHook = getEepromData
	stop.Hook = stopHook
	nld.Prefixes = []string{"lo.", "eth0."}
	vnetd.UnixInterfacesOnly = true
	vnetd.PublishAllCounters = false
	vnetd.GdbWait = gdbwait
	vnetd.Hook = vnetHook
	g.Main()
}

func stopHook() error {
	for port := 0; port < 32; port++ {
		for subport := 0; subport < 4; subport++ {
			exec.Command("/bin/ip", "link", "delete",
				fmt.Sprintf("eth-%d-%d", port, subport),
			).Run()
		}
	}
	for port := 0; port < 2; port++ {
		exec.Command("/bin/ip", "link", "delete",
			fmt.Sprintf("ixge2-0-%d", port),
		).Run()
	}
	for port := 0; port < 2; port++ {
		exec.Command("/bin/ip", "link", "delete",
			fmt.Sprintf("meth-%d", port),
		).Run()
	}
	return nil
}

func vnetHook(i *vnetd.Info, v *govnet.Vnet) error {
	// Base packages.
	ethernet.Init(v)
	ip4.Init(v)
	ip6.Init(v)
	pg.Init(v)   // vnet packet generator
	unix.Init(v) // tuntap/netlink

	// Device drivers: FE1 switch + Intel 10G ethernet for punt path.
	ixge.Init(v)
	fe1.Init(v)

	plat := &platform{i: i}
	v.AddPackage("platform", plat)
	plat.DependsOn("pci-discovery")

	return nil
}

func getEepromData(pub chan<- string) error {
	// The MK1 x86 CPU Card EEPROM is located on bus 0, addr 0x51:
	d := eeprom.Device{
		BusIndex:   0,
		BusAddress: 0x51,
	}

	// Read and store the EEPROM Contents
	if err := d.GetInfo(); err != nil {
		return err
	}

	pub <- fmt.Sprint("eeprom.product_name: ", d.Fields.ProductName)
	pub <- fmt.Sprint("eeprom.platform_name: ", d.Fields.PlatformName)
	pub <- fmt.Sprint("eeprom.manufacturer: ", d.Fields.Manufacturer)
	pub <- fmt.Sprint("eeprom.vendor: ", d.Fields.Vendor)
	pub <- fmt.Sprint("eeprom.part_number: ", d.Fields.PartNumber)
	pub <- fmt.Sprint("eeprom.serial_number: ", d.Fields.SerialNumber)
	pub <- fmt.Sprint("eeprom.device_version: ", d.Fields.DeviceVersion)
	pub <- fmt.Sprint("eeprom.manufacture_date: ", d.Fields.ManufactureDate)
	pub <- fmt.Sprint("eeprom.country_code: ", d.Fields.CountryCode)
	pub <- fmt.Sprint("eeprom.diag_version: ", d.Fields.DiagVersion)
	pub <- fmt.Sprint("eeprom.service_tag: ", d.Fields.ServiceTag)
	pub <- fmt.Sprint("eeprom.base_ethernet_address: ", d.Fields.BaseEthernetAddress)
	pub <- fmt.Sprint("eeprom.number_of_ethernet_addrs: ", d.Fields.NEthernetAddress)
	return nil
}

func wait4vnet() error {
	conn, err := sockfile.Dial("redisd")
	if err != nil {
		return err
	}
	defer conn.Close()
	psc := redigo.PubSubConn{redigo.NewConn(conn, 0, 500*time.Millisecond)}
	if err = psc.Subscribe(goredis.Machine); err != nil {
		return err
	}
	for {
		switch t := psc.Receive().(type) {
		case redigo.Message:
			if string(t.Data) == "vnet.ready: true" {
				return nil
			}
		case error:
			return t
		}
	}
	return nil
}
