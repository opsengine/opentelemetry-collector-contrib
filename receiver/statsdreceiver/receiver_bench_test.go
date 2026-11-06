// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package statsdreceiver

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/statsdreceiver/internal/metadata"
)

const (
	benchLinesPerPacket = 20
	benchSeries         = 200
	benchWindowPackets  = 1000
	benchStallTimeout   = 10 * time.Second
)

// waitFor fails the benchmark if the sink stops making progress, e.g. after kernel packet loss.
func waitFor(b *testing.B, cond func() bool, total *atomic.Int64) {
	b.Helper()
	last, lastChange := total.Load(), time.Now()
	for !cond() {
		if cur := total.Load(); cur != last {
			last, lastChange = cur, time.Now()
		} else if time.Since(lastChange) > benchStallTimeout {
			b.Fatalf("no progress for %s, packets were probably dropped", benchStallTimeout)
		}
		time.Sleep(20 * time.Microsecond)
	}
}

func benchPackets(linesPerPacket int) [][]byte {
	packets := make([][]byte, benchSeries/linesPerPacket)
	for p := range packets {
		var sb strings.Builder
		for l := 0; l < linesPerPacket; l++ {
			s := p*linesPerPacket + l
			fmt.Fprintf(&sb, "svc.request.count%d:1|c|#env:prod,service:api,region:us-east-1,pod:abc\n", s)
		}
		packets[p] = []byte(strings.TrimSuffix(sb.String(), "\n"))
	}
	return packets
}

// countSink sums every counter value so the benchmark can flow-control the sender.
func countSink(total *atomic.Int64, flushDelay time.Duration) consumer.Metrics {
	c, _ := consumer.NewMetrics(func(_ context.Context, md pmetric.Metrics) error {
		var n int64
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					dps := ms.At(k).Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						n += dps.At(d).IntValue()
					}
				}
			}
		}
		if flushDelay > 0 {
			time.Sleep(flushDelay)
		}
		total.Add(n)
		return nil
	})
	return c
}

func benchmarkReceiverUDP(b *testing.B, linesPerPacket int, flushDelay time.Duration) {
	addr := testutil.GetAvailableLocalNetworkAddress(b, "udp")
	cfg := createDefaultConfig().(*Config)
	cfg.NetAddr = confignet.AddrConfig{Endpoint: addr, Transport: confignet.TransportTypeUDP}
	cfg.AggregationInterval = 5 * time.Millisecond
	cfg.SocketBufferSize = 8 * 1024 * 1024

	var total atomic.Int64
	rcv, err := newReceiver(receivertest.NewNopSettings(metadata.Type), *cfg, countSink(&total, flushDelay))
	require.NoError(b, err)
	require.NoError(b, rcv.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(b, rcv.Shutdown(context.Background())) }()

	conn, err := net.Dial("udp", addr)
	require.NoError(b, err)
	defer conn.Close()

	packets := benchPackets(linesPerPacket)
	target := int64(b.N)
	b.ResetTimer()
	var sent int64
	window := int64(benchWindowPackets * linesPerPacket)
	for i := 0; sent < target; i++ {
		waitFor(b, func() bool { return sent-total.Load() < window }, &total)
		_, err := conn.Write(packets[i%len(packets)])
		require.NoError(b, err)
		sent += int64(linesPerPacket)
	}
	waitFor(b, func() bool { return total.Load() >= sent }, &total)
	b.StopTimer()
	b.ReportMetric(float64(sent)/b.Elapsed().Seconds(), "lines/s")
}

func BenchmarkReceiverUDP(b *testing.B) {
	benchmarkReceiverUDP(b, benchLinesPerPacket, 0)
}

func BenchmarkReceiverUDPSinglePerPacket(b *testing.B) {
	benchmarkReceiverUDP(b, 1, 0)
}

// SlowConsumer simulates a downstream that takes 3ms to accept each flush.
func BenchmarkReceiverUDPSlowConsumer(b *testing.B) {
	benchmarkReceiverUDP(b, benchLinesPerPacket, 3*time.Millisecond)
}
