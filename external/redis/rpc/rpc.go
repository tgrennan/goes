// Copyright © 2015-2020 Platina Systems, Inc. All rights reserved.
// Use of this source code is governed by the GPL-2 license described in the
// LICENSE file.

// Package rpc provides remote calls to a redis server.
package rpc

import (
	"github.com/platinasystems/goes/external/atsock"
	"github.com/platinasystems/goes/external/redis/rpc/args"
	"github.com/platinasystems/goes/external/redis/rpc/reply"
)

var empty = struct{}{}

type Rpc struct{ AtSock, Name string }

func New(suffix, name string) *Rpc { return &Rpc{suffix, name} }

func (rpc *Rpc) Del(key string, keys ...string) (int, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	var r reply.Del
	err = cl.Call(rpc.Name+".Del", args.Del{Key: key, Keys: keys}, &r)
	if err != nil {
		return 0, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Get(key string) ([]byte, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var r reply.Get
	err = cl.Call(rpc.Name+".Get", args.Get{Key: key}, &r)
	if err != nil {
		return nil, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Set(key string, value []byte) error {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return err
	}
	defer cl.Close()
	return cl.Call(rpc.Name+".Set", args.Set{Key: key, Value: value}, &empty)
}

func (rpc *Rpc) Hdel(key, field string, fields ...string) (int, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	var r reply.Hdel
	err = cl.Call(rpc.Name+".Hdel", args.Hdel{Key: key, Field: field,
		Fields: fields}, &r)
	if err != nil {
		return 0, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Hexists(key, field string) (int, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	var r reply.Hexists
	err = cl.Call(rpc.Name+".Hexists", args.Hexists{Key: key, Field: field},
		&r)
	if err != nil {
		return 0, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Hget(key, field string) ([]byte, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var r reply.Hget
	err = cl.Call(rpc.Name+".Hget", args.Hget{Key: key, Field: field}, &r)
	if err != nil {
		return nil, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Hgetall(key string) ([][]byte, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var r reply.Hgetall
	err = cl.Call(rpc.Name+".Hgetall", args.Hgetall{Key: key}, &r)
	if err != nil {
		return nil, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Hkeys(key string) ([][]byte, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var r reply.Hkeys
	err = cl.Call(rpc.Name+".Hkeys", args.Hkeys{Key: key}, &r)
	if err != nil {
		return nil, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Hset(key, id string, value []byte) (int, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	var r reply.Hset
	err = cl.Call(rpc.Name+".Hset", args.Hset{Key: key, Field: id, Value: value}, &r)
	if err != nil {
		return 0, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Lrange(key string, start, stop int) ([][]byte, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var r reply.Lrange
	err = cl.Call(rpc.Name+".Lrange", args.Lrange{Key: key, Start: start,
		Stop: stop}, &r)
	if err != nil {
		return nil, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Lindex(key string, index int) ([]byte, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var r reply.Lindex
	err = cl.Call(rpc.Name+".Lindex", args.Lindex{Key: key, Index: index},
		&r)
	if err != nil {
		return nil, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Blpop(key string, keys ...string) ([][]byte, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var r reply.Blpop
	err = cl.Call(rpc.Name+".Blpop", args.Blpop{Key: key, Keys: keys}, &r)
	if err != nil {
		return nil, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Brpop(key string, keys ...string) ([][]byte, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var r reply.Brpop
	err = cl.Call(rpc.Name+".Brpop", args.Brpop{Key: key, Keys: keys}, &r)
	if err != nil {
		return nil, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Lpush(key string, value []byte, values ...[]byte) (int, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	var r reply.Lpush
	err = cl.Call(rpc.Name+".Lpush", args.Lpush{Key: key, Value: value,
		Values: values}, &r)
	if err != nil {
		return 0, err
	}
	return r.Redis(), nil
}

func (rpc *Rpc) Rpush(key string, value []byte, values ...[]byte) (int, error) {
	cl, err := atsock.NewRpcClient(rpc.AtSock)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	var r reply.Rpush
	err = cl.Call(rpc.Name+".Rpush", args.Rpush{Key: key, Value: value,
		Values: values}, &r)
	if err != nil {
		return 0, err
	}
	return r.Redis(), nil
}
