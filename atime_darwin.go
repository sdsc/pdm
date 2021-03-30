package main

import (
	"syscall"
	"time"
)

func getAtime(sourceStat *syscall.Stat_t) time.Time {
	return time.Unix(int64(sourceStat.Atimespec.Sec), int64(sourceStat.Atimespec.Nsec))
}

func getCtime(sourceStat *syscall.Stat_t) time.Time {
	return time.Unix(int64(sourceStat.Ctimespec.Sec), int64(sourceStat.Ctimespec.Nsec))
}
