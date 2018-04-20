// Copyright © 2015-2018 Platina Systems, Inc. All rights reserved.
// Use of this source code is governed by the GPL-2 license described in the
// LICENSE file.

package bootd

const (
	Register  = "register"
	DumpVars  = "dumpvars"
	Dashboard = "dashboard"
)
const (
	BootStateNotRegistered = iota
	BootStateRegistered
	BootStateBooting
	BootStateUp
	BootStateInstalling
	BootStateIntstallFailed
	BootStateRebooting
)
const (
	InstallStateFactory = iota
	InstallStateInProgress
	InstallStateInstalled
	InstallStateInstallFail
	InstallStateFactoryInProgress
	InstallStateFactoryFailed
)
const (
	Debian = iota
)
const (
	RegReplyFound = iota
	RegReplyNotFound
)
const (
	ScriptBootLatest = iota
	ScriptBootKnownGood
	ScriptInstallDebian
)
const (
	BootReplyNormal = iota
	BootReplyRunGoesScript
	BootReplyExecUsermode
	BootReplyExecKernel
	BootReplyReflashAndReboot
)

type Client struct {
	unit           int
	name           string
	machine        string
	macAddr        string
	ipAddr         string
	bootState      int
	installState   int
	autoInstall    bool
	certPresent    bool
	distroType     int
	timeRegistered string
	timeInstalled  string
	installCounter int
}

type RegReq struct {
	Mac string
	IP  string
}

type RegReply struct {
	Reply   int
	TorName string
	Error   error
}

type BootReq struct {
	Images []string
}

type BootReply struct {
	Reply      int
	ImageName  string
	Script     string
	ScriptType string
	Binary     []byte
	Error      error
}

var ClientCfg map[string]*Client
var regReq RegReq
var regReply RegReply

func bootText(i int) string {
	var bootStates = []string{
		"Not-Registered",
		"Registered",
		"Booting",
		"Up",
		"Installing",
		"Rebooting",
	}
	return bootStates[i]
}

func installText(i int) string {
	var installStates = []string{
		"Factory",
		"Install-in-progress",
		"Installed",
		"Install-failed",
		"Restore-in-progress",
		"Restore-failed",
	}
	return installStates[i]
}

func distroText(i int) string {
	var distroTypes = []string{
		"Debian",
	}
	return distroTypes[i]
}

func scriptText(i int) string {
	var scripts = []string{
		"Boot-latest",
		"Boot-known-good",
		"Debian-install",
	}
	return scripts[i]
}