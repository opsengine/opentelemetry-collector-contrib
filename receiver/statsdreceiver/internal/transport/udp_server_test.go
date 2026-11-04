// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//go:build linux

package transport

import (
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
)

func Test_NewUDPServer_SocketBufferSize(t *testing.T) {
	addr := testutil.GetAvailableLocalNetworkAddress(t, "udp")
	bufferSize := 2 * 1024 * 1024 // 2MB

	server, err := NewUDPServer(UDP, addr, bufferSize)
	require.NoError(t, err)
	require.NotNil(t, server)
	defer server.Close()

	uc, ok := server.(*udpServer).packetConn.(*net.UDPConn)
	require.True(t, ok)
	raw, err := uc.SyscallConn()
	require.NoError(t, err)
	var actual int
	err = raw.Control(func(fd uintptr) {
		actual, _ = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	})
	require.NoError(t, err)
	// The kernel may round up, so just check it's at least what we asked for.
	assert.GreaterOrEqual(t, actual, bufferSize,
		"SO_RCVBUF should be at least the requested size")
}

func Test_NewUDPServer_DefaultSocketBufferSize(t *testing.T) {
	addr := testutil.GetAvailableLocalNetworkAddress(t, "udp")

	// socketBufferSize=0 means don't change the OS default.
	server, err := NewUDPServer(UDP, addr, 0)
	require.NoError(t, err)
	require.NotNil(t, server)
	server.Close()
}
