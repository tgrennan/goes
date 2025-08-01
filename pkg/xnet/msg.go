// Copyright © 2023-2025 Platina Systems, Inc. All rights reserved.
// Use of this source code is governed by the GPL-2 license described in the
// LICENSE file.

package xnet

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
)

var zap netip.AddrPort

type AddrPorter interface {
	AddrPort() netip.AddrPort
}

type RemoteAddrer interface {
	RemoteAddr() net.Addr
}

type Msg struct {
	next *Msg
	netip.AddrPort
	Data []byte
}

type MsgPool struct {
	mtu int
	p   atomic.Pointer[Msg]
}

func NewMsgPool(mtu int) *MsgPool {
	return &MsgPool{
		mtu: mtu,
	}
}

func (mp *MsgPool) Get() *Msg {
	for {
		m := mp.p.Load()
		if m == nil {
			return &Msg{Data: make([]byte, mp.mtu, mp.mtu)}
		}
		if mp.p.CompareAndSwap(m, m.next) {
			m.next = nil
			return m
		}
	}
}

func (mp *MsgPool) Put(m *Msg) {
	m.AddrPort = zap
	if cap(m.Data) != mp.mtu {
		// Let GC deal with this.
		m.Data = m.Data[:0]
		m = nil
		return
	}
	m.Data = m.Data[:mp.mtu]
	for {
		m.next = mp.p.Load()
		if mp.p.CompareAndSwap(m.next, m) {
			break
		}
	}
}

// Discard message and return false if context is done before message is
// channeled; otherwise return true.
func (mp *MsgPool) Queue(ctx context.Context, ch chan<- *Msg, m *Msg) bool {
	select {
	case <-ctx.Done():
		mp.Put(m)
	case ch <- m:
		return true
	}
	return false
}
