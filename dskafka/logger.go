package dskafka

import (
	"context"
	"log"
)

// Logger is the interface the SDK uses to report internal errors (e.g. failed
// sends, read errors, offset failures). Implementations must be safe for
// concurrent use.
//
// Set Config.Logger to enable logging; when left nil, the SDK produces no
// log output at all. Use StdLogger for the SDK's previous behavior of
// writing to the standard library's logger, or provide your own
// implementation to route SDK errors through your application's logger.
type Logger interface {
	Error(ctx context.Context, format string, args ...any)
}

// noopLogger discards all log output. It is the default used whenever
// Config.Logger is not set, so importing services never see SDK-internal
// log output unless they opt in.
type noopLogger struct{}

func (noopLogger) Error(context.Context, string, ...any) {}

// StdLogger is a Logger implementation that writes to Go's standard library
// log package (stderr by default). Set Config.Logger to &StdLogger{} to
// restore the SDK's previous unconditional logging behavior.
type StdLogger struct{}

func (StdLogger) Error(_ context.Context, format string, args ...any) {
	log.Printf("ERROR "+format, args...)
}

// resolveLogger returns l, or a no-op Logger if l is nil.
func resolveLogger(l Logger) Logger {
	if l == nil {
		return noopLogger{}
	}
	return l
}
