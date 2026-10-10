// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

// Build this small server in the legacy alarmd module to exercise its actual
// authorization handler against the new instance's shared fixture Redis.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cliauth"
	"github.com/go-redis/redis/v8"
)

func main() {
	client := redis.NewClient(&redis.Options{Addr: os.Args[1], MaxRetries: -1})
	if err := client.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}
	manager, err := cliauth.New(cliauth.Options{Redis: client, Prefix: os.Args[2], EnvironmentID: os.Args[3], EnvironmentName: "Legacy fixture", PublicBaseURL: os.Args[4], AdminKey: os.Args[5]})
	if err != nil {
		panic(err)
	}
	socket, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	fmt.Println("http://" + socket.Addr().String())
	if err := http.Serve(socket, manager.Handler()); err != nil {
		panic(err)
	}
}
