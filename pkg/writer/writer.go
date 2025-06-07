package writer

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"io"
)

type Writer interface {
	Write(*labels.Labels, []int64, []float64) error
}

func NewWriter(format string, out io.Writer) (Writer, error) {
	switch format {
	case "victoriametrics":
		return NewVictoriaMetricsWriter(out)
	}
	return nil, fmt.Errorf("invalid format: %s", format)
}
