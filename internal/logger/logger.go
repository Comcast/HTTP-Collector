package logger

import (
	"context"
	"fmt"
	"io"
	"os"

	"log/slog"
)

type Logger struct {
	*slog.Logger
	lvlVar *slog.LevelVar
}

func NewLogger() *Logger {
	lvlVar := new(slog.LevelVar)
	return &Logger{
		Logger: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvlVar})),
		lvlVar: lvlVar,
	}
}

func NewLoggerWithIOWriter(w io.Writer) *Logger {
	lvlVar := new(slog.LevelVar)
	return &Logger{
		Logger: slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvlVar})),
		lvlVar: lvlVar,
	}
}

func (l *Logger) Fatal(msg string, args ...any) {
	l.Error(msg, args...)
	os.Exit(1)
}

func (l *Logger) Level() slog.Level {
	return l.lvlVar.Level()
}

func (l *Logger) Print(v ...any) {
	l.LogAttrs(context.Background(), l.lvlVar.Level(), fmt.Sprint(v...))
}

func (l *Logger) Println(v ...any) {
	l.LogAttrs(context.Background(), l.lvlVar.Level(), fmt.Sprint(v...))
}

func (l *Logger) Printf(format string, v ...any) {
	l.LogAttrs(context.Background(), l.lvlVar.Level(), fmt.Sprintf(format, v...))
}

func (l *Logger) SetLevel(level slog.Level) {
	l.lvlVar.Set(level)
}

func (l *Logger) Slog() *slog.Logger {
	return l.Logger
}

func (l *Logger) With(args ...any) *Logger {
	if len(args) == 0 {
		return l
	}
	c := *l
	c.Logger = l.Logger.With(args...)
	return &c
}

func (l *Logger) WithGroup(name string) *Logger {
	if name == "" {
		return l
	}
	c := *l
	c.Logger = l.Logger.WithGroup(name)
	return &c
}
