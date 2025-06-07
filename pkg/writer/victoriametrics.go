package writer

import (
	"encoding/json"
	"github.com/prometheus/prometheus/pkg/labels"
	"io"
)

type VictoriaMetricsWriter struct {
	enc *json.Encoder
}

func NewVictoriaMetricsWriter(out io.Writer) (*VictoriaMetricsWriter, error) {
	return &VictoriaMetricsWriter{enc: json.NewEncoder(out)}, nil
}

type victoriaMetricsLine struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func (w *VictoriaMetricsWriter) Write(labels *labels.Labels, timestamps []int64, values []float64) error {
	metric := map[string]string{}
	for _, l := range *labels {
		metric[l.Name] = l.Value
	}

	err := w.enc.Encode(victoriaMetricsLine{
		Metric:     metric,
		Values:     values,
		Timestamps: timestamps,
	})
	if err != nil {
		return err
	}
	return nil
}
