package bootstrap

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"globalmail/config"
)

const DefaultConfigPath = "config/examples/globalmail.yaml"

func ConfigPathFlag(defaultPath string) *string {
	if defaultPath == "" {
		defaultPath = DefaultConfigPath
	}
	return flag.String("config", defaultPath, "path to YAML config file")
}

func LoadConfig(path string) (config.Config, error) {
	return config.LoadFile(path)
}

func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}
