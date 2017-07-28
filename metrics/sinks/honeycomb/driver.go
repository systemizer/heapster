package honeycomb

import (
	"net/url"
	"sync"

	"github.com/golang/glog"
	honeycomb_common "k8s.io/heapster/common/honeycomb"
	"k8s.io/heapster/metrics/core"
)

// These metrics report cumulative values over the lifetime of the process.
// Heapster also reports gauges (e.g., "cpu/usage_rate"). Cumulative metrics
// are more confusing than helpful, so let's not send them in the first place.
var blacklist = map[string]struct{}{
	"cpu/usage":                {},
	"memory/major_page_faults": {},
	"memory/page_faults":       {},
	"network/rx_errors":        {},
	"network/rx":               {},
	"network/tx_errors":        {},
	"network/tx":               {},
}

type honeycombSink struct {
	client *honeycomb_common.Client
	sync.Mutex
}

type Point struct {
	MetricsName string
	MetricsTags string
}

func (sink *honeycombSink) ExportData(dataBatch *core.DataBatch) {

	sink.Lock()
	defer sink.Unlock()

	batch := make(honeycomb_common.Batch, len(dataBatch.MetricSets))

	i := 0
	for _, metricSet := range dataBatch.MetricSets {
		data := make(map[string]interface{})
		for metricName, metricValue := range metricSet.MetricValues {
			if _, ok := blacklist[metricName]; ok {
				continue
			}
			data[metricName] = metricValue.GetValue()
		}
		for k, v := range metricSet.Labels {
			data[k] = v
		}
		batch[i] = &honeycomb_common.BatchPoint{
			Data:      data,
			Timestamp: dataBatch.Timestamp,
		}
		i++
	}
	err := sink.client.SendBatch(batch)
	if err != nil {
		glog.Warningf("Failed to send metrics batch: %v", err)
	}
}

func (sink *honeycombSink) Stop() {}
func (sink *honeycombSink) Name() string {
	return "Honeycomb Sink"
}

func NewHoneycombSink(uri *url.URL) (core.DataSink, error) {
	client, err := honeycomb_common.NewClient(uri)
	if err != nil {
		return nil, err
	}
	sink := &honeycombSink{
		client: client,
	}

	return sink, nil
}
