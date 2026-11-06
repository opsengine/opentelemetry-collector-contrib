// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package transport // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/statsdreceiver/internal/transport"

import (
	"errors"
	"fmt"
	"net"

	"go.opentelemetry.io/collector/consumer"
)

type packetServer struct {
	packetConn net.PacketConn
	transport  Transport
}

// setReadBuffer applies SO_RCVBUF when size > 0; 0 keeps the OS default.
func setReadBuffer(conn net.PacketConn, size int) error {
	if size <= 0 {
		return nil
	}
	rb, ok := conn.(interface{ SetReadBuffer(int) error })
	if !ok {
		return nil
	}
	if err := rb.SetReadBuffer(size); err != nil {
		return fmt.Errorf("setting socket buffer size: %w", err)
	}
	return nil
}

// ListenAndServe starts the server ready to receive metrics.
func (u *packetServer) ListenAndServe(
	nextConsumer consumer.Metrics,
	reporter Reporter,
	transferChan chan<- Metric,
) error {
	if nextConsumer == nil || reporter == nil {
		return errNilListenAndServeParameters
	}

	buf := make([]byte, 65527) // max size for udp packet body (assuming ipv6)
	for {
		n, addr, err := u.packetConn.ReadFrom(buf)
		if addr == nil && u.transport == UDS {
			addr = &udsAddr{
				network: u.transport.String(),
				address: u.packetConn.LocalAddr().String(),
			}
		}

		if n > 0 {
			u.handlePacket(n, buf, addr, transferChan)
		}
		if err != nil {
			reporter.OnDebugf("%s Transport (%s) - ReadFrom error: %v",
				u.transport,
				u.packetConn.LocalAddr(),
				err)
			if netErr, ok := errors.AsType[net.Error](err); ok {
				if netErr.Timeout() {
					continue
				}
			}
			return err
		}
	}
}

// handlePacket forwards the whole packet as one Metric, the consumer splits it into lines.
func (*packetServer) handlePacket(
	numBytes int,
	data []byte,
	addr net.Addr,
	transferChan chan<- Metric,
) {
	transferChan <- Metric{string(data[:numBytes]), addr}
}

type udsAddr struct {
	network string
	address string
}

func (u *udsAddr) Network() string {
	return u.network
}

func (u *udsAddr) String() string {
	return u.address
}
