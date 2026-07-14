package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
)

// ResultWriter is a thin wrapper around encoding/csv so each scenario can
// dump its results to loadtest/results/<name>.csv without repeating the
// boilerplate of creating the directory, the file, and flushing on close.
type ResultWriter struct {
	f *os.File
	w *csv.Writer
}

func NewResultWriter(path string, header []string) (*ResultWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		f.Close()
		return nil, err
	}

	return &ResultWriter{f: f, w: w}, nil
}

func (rw *ResultWriter) WriteRow(fields ...string) error {
	return rw.w.Write(fields)
}

func (rw *ResultWriter) Close() error {
	rw.w.Flush()
	if err := rw.w.Error(); err != nil {
		rw.f.Close()
		return err
	}
	return rw.f.Close()
}
