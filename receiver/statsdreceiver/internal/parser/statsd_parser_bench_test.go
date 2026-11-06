// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parser

import (
	"fmt"
	"net"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/statsdreceiver/protocol"
)

var benchAddr = &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5000}

func benchLines(n int, format string) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(format, i%200, i)
	}
	return out
}

func benchAggregate(b *testing.B, p *StatsDParser, lines []string) {
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = p.Aggregate(lines[i%len(lines)], benchAddr)
		i++
	}
}

func BenchmarkParseMessageToMetric(b *testing.B) {
	line := "svc.request.count:1|c|#env:prod,service:api,region:us-east-1,pod:abc"
	b.ReportAllocs()
	for b.Loop() {
		_, _ = parseMessageToMetric(line, true, true)
	}
}

func BenchmarkAggregateCounter(b *testing.B) {
	p := &StatsDParser{}
	_ = p.Initialize(true, true, false, false, false, nil, "")
	benchAggregate(b, p, benchLines(1000, "svc.count%d:%d|c|#env:prod,service:api,region:us-east-1,pod:abc"))
}

func BenchmarkAggregateGauge(b *testing.B) {
	p := &StatsDParser{}
	_ = p.Initialize(true, true, false, false, false, nil, "")
	benchAggregate(b, p, benchLines(1000, "svc.gauge%d:%d|g|#env:prod,service:api,pod:abc"))
}

func BenchmarkAggregateHistogram(b *testing.B) {
	p := &StatsDParser{}
	mapping := []protocol.TimerHistogramMapping{
		{StatsdType: protocol.HistogramTypeName, ObserverType: protocol.HistogramObserver},
		{StatsdType: protocol.TimingTypeName, ObserverType: protocol.HistogramObserver},
		{StatsdType: protocol.DistributionTypeName, ObserverType: protocol.HistogramObserver},
	}
	_ = p.Initialize(true, true, false, false, false, mapping, "")
	benchAggregate(b, p, benchLines(1000, "svc.lat%d:%d.5|d|#env:prod,service:api"))
}

func BenchmarkGetMetrics(b *testing.B) {
	p := &StatsDParser{}
	_ = p.Initialize(true, true, false, false, false, nil, "")
	lines := benchLines(5000, "svc.count%d:%d|c|#env:prod,service:api,pod:abc")
	for i := range lines {
		lines[i] = fmt.Sprintf("svc.count%d:1|c|#env:prod,service:api,pod:p%d", i, i%50)
	}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		for _, l := range lines {
			_ = p.Aggregate(l, benchAddr)
		}
		b.StartTimer()
		_ = p.GetMetrics()
	}
}

func BenchmarkAggregateCounterUniqueTags(b *testing.B) {
	p := &StatsDParser{}
	_ = p.Initialize(true, true, false, false, false, nil, "")
	lines := make([]string, 100000)
	for i := range lines {
		lines[i] = fmt.Sprintf("svc.count%d:1|c|#env:prod,service:api,pod:p%d", i%200, i)
	}
	benchAggregate(b, p, lines)
}
