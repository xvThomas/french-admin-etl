package logger

import (
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

var Logger *slog.Logger

func init() {
	godotenv.Load()
	level := getLogLevel()

	opts := &slog.HandlerOptions{
		Level: level,
	}

	// Choose the format based on the env variable LOG_FORMAT
	var handler slog.Handler
	if os.Getenv("LOG_FORMAT") == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	Logger = slog.New(handler)
	slog.SetDefault(Logger)
}

func getLogLevel() slog.Level {
	// Default log level is INFO, can be overridden by LOG_LEVEL env variable
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func GetLogger() *slog.Logger {
	return Logger
}
