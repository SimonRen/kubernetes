/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"strings"
	"sync"
	"testing"
)

func TestLineWriter_SingleNewlineConsumed(t *testing.T) {
	w := newLineWriter("f", "stdout")
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if w.buf.Len() != 0 {
		t.Fatalf("buf should drain on '\\n', got %q", w.buf.String())
	}
}

func TestLineWriter_PartialBufferedAcrossWrites(t *testing.T) {
	w := newLineWriter("f", "stdout")
	w.Write([]byte("hel"))
	if w.buf.String() != "hel" {
		t.Fatalf("partial not buffered; got %q", w.buf.String())
	}
	w.Write([]byte("lo\nworld"))
	if w.buf.String() != "world" {
		t.Fatalf("after newline, partial after '\\n' should remain; got %q", w.buf.String())
	}
}

func TestLineWriter_MultipleLinesInOneWrite(t *testing.T) {
	w := newLineWriter("f", "stdout")
	w.Write([]byte("a\nb\nc\n"))
	if w.buf.Len() != 0 {
		t.Fatalf("buf should drain on multiple newlines; got %q", w.buf.String())
	}
}

func TestLineWriter_HugeNoNewlineDoesNotPanic(t *testing.T) {
	w := newLineWriter("f", "stdout")
	payload := strings.Repeat("x", 100*1024) // 100 KB without '\n'
	n, err := w.Write([]byte(payload))
	if err != nil || n != len(payload) {
		t.Fatalf("Write(huge) returned n=%d err=%v", n, err)
	}
	if w.buf.Len() != len(payload) {
		t.Fatalf("expected all bytes buffered, got %d", w.buf.Len())
	}
}

func TestLineWriter_FlushEmitsTrailingPartial(t *testing.T) {
	w := newLineWriter("f", "stdout")
	w.Write([]byte("partial"))
	w.flush()
	if w.buf.Len() != 0 {
		t.Fatalf("flush should drain; got %q", w.buf.String())
	}
}

func TestLineWriter_CRLFTrimmed(t *testing.T) {
	w := newLineWriter("f", "stdout")
	w.Write([]byte("hello\r\n"))
	if w.buf.Len() != 0 {
		t.Fatalf("CRLF should consume the line; got %q", w.buf.String())
	}
}

func TestLineWriter_ConcurrentWritesRaceFree(t *testing.T) {
	// Run with 'go test -race' to verify the mutex covers the buffer access.
	w := newLineWriter("f", "stdout")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				w.Write([]byte("hello\n"))
			}
		}()
	}
	wg.Wait()
}

func TestLineWriter_EmptyLinesAreNoop(t *testing.T) {
	w := newLineWriter("f", "stdout")
	// Lots of newlines with nothing between them — should not panic, should
	// drain the buffer.
	w.Write([]byte("\n\n\n"))
	if w.buf.Len() != 0 {
		t.Fatalf("buf should drain on bare newlines; got %q", w.buf.String())
	}
}
