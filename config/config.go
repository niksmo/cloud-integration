package config

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	genTickFlag    = "gen-tick"
	genTickDefault = 3 * time.Second

	logLevelFlag        = "log"
	logLevelFlagDefault = slog.LevelInfo
)

type Config struct {
	LogLevel        slog.Level
	PaymentsGenTick time.Duration
}

func Load() Config {
	initArgs()
	initEnv()
	return Config{
		LogLevel:        slog.Level(viper.GetInt(logLevelFlag)),
		PaymentsGenTick: viper.GetDuration(genTickFlag),
	}
}

func initArgs() {
	cmdLine := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)

	cmdLine.Duration(genTickFlag, genTickDefault, "payments generator tick duration")
	cmdLine.Int(logLevelFlag, int(logLevelFlagDefault), "log level (default \"INFO\")")

	_ = cmdLine.Parse(os.Args[1:])
	viper.BindPFlags(cmdLine)
}

func initEnv() {
	_ = viper.BindEnv(genTickFlag, getEnvVar(genTickFlag))
	_ = viper.BindEnv(logLevelFlag, getEnvVar(logLevelFlag))
}

func getEnvVar(input string) string {
	res := []string{"CLOUD"}
	for s := range strings.SplitSeq(input, "-") {
		res = append(res, strings.ToUpper(s))
	}
	return strings.Join(res, "_")
}
