package writer

import (
	"encoding/csv"
	"os"
	"sort"
	"strconv"

	"github.com/prometheus/prometheus/pkg/labels"
)

// CSVWriter writes samples in CSV format.
type CSVWriter struct {
	enc *csv.Writer
}

// NewCSVWriter creates a writer that outputs CSV to stdout.
func NewCSVWriter() (*CSVWriter, error) {
	return &CSVWriter{enc: csv.NewWriter(os.Stdout)}, nil
}

// Write writes the given samples as CSV rows. Each row consists of the metric
// name, timestamp, value and the values of other labels sorted by their name.
func (w *CSVWriter) Write(lbls *labels.Labels, timestamps []int64, values []float64) error {
	var name string
	other := make([]labels.Label, 0, len(*lbls))
	for _, l := range *lbls {
		if l.Name == "__name__" {
			name = l.Value
			continue
		}
		other = append(other, l)
	}
	sort.Slice(other, func(i, j int) bool { return other[i].Name < other[j].Name })

	for i := range timestamps {
		row := make([]string, 0, 3+len(other))
		row = append(row, name, strconv.FormatInt(timestamps[i], 10), strconv.FormatFloat(values[i], 'f', -1, 64))
		for _, l := range other {
			row = append(row, l.Value)
		}
		if err := w.enc.Write(row); err != nil {
			return err
		}
	}
	w.enc.Flush()
	return w.enc.Error()
}
