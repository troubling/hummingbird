package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func systemdCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("systemd with no additional args")
	}
	switch args[0] {
	case "start":
		for {
			serverExecutable, err := exec.LookPath(os.Args[0])
			if err != nil {
				return fmt.Errorf("systemd unable to find executable in path: %q", os.Args[0])
			}
			cmd := exec.Command(serverExecutable, args[1:]...)
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Start()
			go func() {
				if err := cmd.Wait(); err != nil {
					fmt.Printf("systemd got error from subcommand: %s", err)
				}
			}()
			sigchan := make(chan os.Signal, 1)
			signal.Notify(sigchan, syscall.SIGHUP, syscall.SIGTERM)
			sig := <-sigchan
			cmd.Process.Signal(sig)
			if sig != syscall.SIGHUP {
				return nil
			}
		}
	case "reload", "stop":
		if len(args) < 2 {
			return fmt.Errorf("systemd %s called with no pid", args[0])
		}
		pid, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("systemd could not parse pid: %q", args[1])
		}
		process, err := os.FindProcess(pid)
		if err != nil {
			return fmt.Errorf("systemd could not find process for pid: %d", pid)
		}
		switch args[0] {
		case "reload":
			process.Signal(syscall.SIGHUP)
			return nil
		case "stop":
			process.Signal(syscall.SIGTERM)
			for i := 0; i < 50; i++ {
				if err = process.Signal(syscall.Signal(0)); err != nil {
					return nil
				}
				time.Sleep(time.Second / 10)
			}
			return fmt.Errorf("systemd gave waiting for pid %d to exit", pid)
		}
	}
	return fmt.Errorf("systemd given unknown subcommand: %s", args[0])
}
