package fakeswift

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os/exec"
)

type FakeSwift struct {
	cmd           *exec.Cmd
	stdoutScanner *bufio.Scanner
	stderrScanner *bufio.Scanner
}

func NewFakeSwift(port int) (f *FakeSwift, err error) {
	f = &FakeSwift{}

	f.cmd = exec.Command("node")

	stdin, err := f.cmd.StdinPipe()

	if err != nil {
		return
	}

	stdout, err := f.cmd.StdoutPipe()

	if err != nil {
		return
	}

	stderr, err := f.cmd.StderrPipe()

	if err != nil {
		return
	}

	err = f.cmd.Start()

	if err != nil {
		return
	}

	stdin.Write([]byte(fmt.Sprintf(FakeSwiftJs, port)))
	stdin.Close()

	stdoutScanner := bufio.NewScanner(stdout)

	if sc := stdoutScanner.Scan(); !sc || stdoutScanner.Text() != "RUNNING" {
		errMsg, _ := ioutil.ReadAll(stderr)
		err = fmt.Errorf("error running fakeswift: %s", string(errMsg))
		return
	}

	stderrScanner := bufio.NewScanner(stderr)

	f.stdoutScanner = stdoutScanner
	f.stderrScanner = stderrScanner

	return
}

func (f *FakeSwift) Close() (err error) {
	return f.cmd.Process.Kill()
}

func (f *FakeSwift) Verbose() {
	go func() {
		for f.stdoutScanner.Scan() {
			fmt.Println("[FakeSwift] " + f.stdoutScanner.Text())
		}
	}()

	go func() {
		for f.stderrScanner.Scan() {
			fmt.Println("[FakeSwift] " + f.stderrScanner.Text())
		}
	}()
}
