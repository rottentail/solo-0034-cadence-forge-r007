package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Root, Address, FFmpeg, FFprobe string
	Workers, QueueLimit            int
	UploadLimit                    int64
	UploadTTL                      time.Duration
}

func env(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
func Load() (Config, error) {
	c := Config{Root: env("FORGE_ROOT", "var/cadence-forge"), Address: env("FORGE_ADDR", "127.0.0.1:8734"), FFmpeg: env("FFMPEG_BIN", "ffmpeg"), FFprobe: env("FFPROBE_BIN", "ffprobe"), Workers: 2, QueueLimit: 16, UploadLimit: 64 << 20, UploadTTL: 24 * time.Hour}
	if value := os.Getenv("FORGE_WORKERS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 8 {
			return c, fmt.Errorf("FORGE_WORKERS must be 1..8")
		}
		c.Workers = n
	}
	if value := os.Getenv("FORGE_UPLOAD_TTL_HOURS"); value != "" {
		hours, err := strconv.Atoi(value)
		if err != nil || hours < 1 || hours > 24*30 {
			return c, fmt.Errorf("FORGE_UPLOAD_TTL_HOURS must be 1..720")
		}
		c.UploadTTL = time.Duration(hours) * time.Hour
	}
	return c, nil
}
