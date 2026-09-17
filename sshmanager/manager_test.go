package sshmanager

import (
	"bytes"
	"sync"
	"testing"
)

func TestShellQuote(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "''"},
		{"simple", "'simple'"},
		{"hello world", "'hello world'"},
		{"it's", "'it'\"'\"'s'"},
		{"~", "\"$HOME\""},
		{"~/file.txt", "\"$HOME\"/'file.txt'"},
		{"~/path/to/'test'", "\"$HOME\"/'path/to/'\"'\"'test'\"'\"''"},
	}

	for _, tc := range tests {
		got := shellQuote(tc.input)
		if got != tc.expected {
			t.Errorf("shellQuote(%q) = %q; want %q", tc.input, got, tc.expected)
		}
	}
}

func TestLimitedWriter(t *testing.T) {
	var buf bytes.Buffer
	limit := 10
	lw := &LimitedWriter{W: &buf, N: limit}

	// Write within limit
	n, err := lw.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("expected 5 bytes written with no error, got %d, %v", n, err)
	}

	// Write exceeding limit without abort
	n, err = lw.Write([]byte(" world! this is extra text"))
	if err != nil {
		t.Fatalf("expected nil error on truncation, got %v", err)
	}
	if n != len([]byte(" world! this is extra text")) {
		t.Fatalf("expected n to match original input slice len, got %d", n)
	}

	if buf.Len() != limit {
		t.Fatalf("expected buffer length %d, got %d", limit, buf.Len())
	}
	if buf.String() != "hello worl" {
		t.Fatalf("expected 'hello worl', got %q", buf.String())
	}

	// Test AbortOnLimit
	var bufAbort bytes.Buffer
	lwAbort := &LimitedWriter{W: &bufAbort, N: limit, AbortOnLimit: true}
	_, err = lwAbort.Write([]byte("1234567890extra"))
	if err == nil {
		t.Fatalf("expected error on exceeding limit when AbortOnLimit is true")
	}
}

func TestSyncWriterConcurrent(t *testing.T) {
	var buf bytes.Buffer
	var bufMu sync.Mutex
	sw := &syncWriter{w: &buf, mu: &bufMu}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = sw.Write([]byte("abcdefghij"))
		}()
	}
	wg.Wait()

	if buf.Len() != 200 {
		t.Fatalf("expected 200 bytes written, got %d", buf.Len())
	}
}
