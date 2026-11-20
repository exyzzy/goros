package goros

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"bitbucket.org/goros/msgs/rcl_interfaces"
)

// LogLevel mirrors rcl_interfaces/msg/Log severities.
type LogLevel uint8

const (
	LogDebug LogLevel = 10
	LogInfo  LogLevel = 20
	LogWarn  LogLevel = 30
	LogError LogLevel = 40
	LogFatal LogLevel = 50
)

func (l LogLevel) String() string {
	switch l {
	case LogDebug:
		return "DEBUG"
	case LogInfo:
		return "INFO"
	case LogWarn:
		return "WARN"
	case LogError:
		return "ERROR"
	case LogFatal:
		return "FATAL"
	}
	return "UNSET"
}

// ParseLogLevel parses "debug", "info", ... (case-insensitive).
func ParseLogLevel(s string) (LogLevel, bool) {
	switch strings.ToLower(s) {
	case "debug":
		return LogDebug, true
	case "info":
		return LogInfo, true
	case "warn", "warning":
		return LogWarn, true
	case "error":
		return LogError, true
	case "fatal":
		return LogFatal, true
	}
	return 0, false
}

// Logger writes console output in the rcutils format
//
//	[INFO] [1700000000.123456789] [talker]: Publishing: 'Hello'
//
// and, for node loggers, forwards records to the /rosout topic.
type Logger struct {
	name   string
	ctx    *Context
	node   *Node // nil for context-level loggers
	mu     sync.Mutex
	level  LogLevel
	rosout *Publisher[*rcl_interfaces.Log]
}

// Name returns the logger name.
func (l *Logger) Name() string { return l.name }

// SetLevel sets the minimum severity this logger emits.
func (l *Logger) SetLevel(lvl LogLevel) {
	l.mu.Lock()
	l.level = lvl
	l.mu.Unlock()
}

// Level returns the current threshold.
func (l *Logger) Level() LogLevel {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// Child returns a logger named "<name>.<suffix>" sharing this logger's sinks.
func (l *Logger) Child(suffix string) *Logger {
	return &Logger{name: l.name + "." + suffix, ctx: l.ctx, node: l.node, level: l.Level(), rosout: l.rosout}
}

func (l *Logger) Log(lvl LogLevel, text string) {
	if lvl < l.Level() {
		return
	}
	now := time.Now()
	w := l.ctx.logWriter()
	if w != nil {
		var b strings.Builder
		b.Grow(len(text) + 64)
		b.WriteByte('[')
		b.WriteString(lvl.String())
		b.WriteString("] [")
		b.WriteString(strconv.FormatInt(now.Unix(), 10))
		b.WriteByte('.')
		ns := strconv.Itoa(now.Nanosecond())
		for i := len(ns); i < 9; i++ {
			b.WriteByte('0')
		}
		b.WriteString(ns)
		b.WriteString("] [")
		b.WriteString(l.name)
		b.WriteString("]: ")
		b.WriteString(text)
		b.WriteByte('\n')
		l.ctx.logMu.Lock()
		io.WriteString(w, b.String())
		l.ctx.logMu.Unlock()
	}
	if l.rosout != nil {
		_ = l.rosout.Publish(&rcl_interfaces.Log{Stamp: TimeFrom(now), Level: uint8(lvl), Name: l.name, Msg: text})
	}
}

func (l *Logger) Debug(text string) { l.Log(LogDebug, text) }
func (l *Logger) Info(text string)  { l.Log(LogInfo, text) }
func (l *Logger) Warn(text string)  { l.Log(LogWarn, text) }
func (l *Logger) Error(text string) { l.Log(LogError, text) }
func (l *Logger) Fatal(text string) { l.Log(LogFatal, text) }

// The f variants format with fmt. On very small targets prefer the plain
// variants with strconv to keep fmt out of the binary.
func (l *Logger) Debugf(format string, args ...any) { l.logf(LogDebug, format, args) }
func (l *Logger) Infof(format string, args ...any)  { l.logf(LogInfo, format, args) }
func (l *Logger) Warnf(format string, args ...any)  { l.logf(LogWarn, format, args) }
func (l *Logger) Errorf(format string, args ...any) { l.logf(LogError, format, args) }
func (l *Logger) Fatalf(format string, args ...any) { l.logf(LogFatal, format, args) }

func (l *Logger) logf(lvl LogLevel, format string, args []any) {
	if lvl < l.Level() {
		return
	}
	l.Log(lvl, fmt.Sprintf(format, args...))
}
