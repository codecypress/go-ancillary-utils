package cypressutils

import (
	"errors"
	"fmt"
	cErrors "github.com/pkg/errors"
	"runtime"
	"runtime/debug"
	"time"
)

type stackTracer interface {
	StackTrace() cErrors.StackTrace
}

func RecoverPanic(err error) {
	if r := recover(); r != nil {
		fmt.Println(r)
		err = errors.New(fmt.Sprintf("%v", r))
		debug.PrintStack()
	}
}

func ThrowException(mainError error) {

	if err, ok := mainError.(stackTracer); ok {
		logLnError(err)

		for _, f := range err.StackTrace() {
			logLnError(fmt.Sprintf("%+s:%d", f, f))
		}
	} else {
		logLnError(mainError)
	}

	pcs := make([]uintptr, 10)
	n := runtime.Callers(2, pcs)
	pcs = pcs[:n]

	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()
		if !more {
			break
		}

		strError := fmt.Sprintf(" at %s(%s:%d)\n", frame.Function, getFilename(frame.File), frame.Line)

		logfError(strError)
	}

}

func logfError(args ...interface{}) {
	tempStr := fmt.Sprintf(" [%s]: [%s] - ", "ERROR", time.Now().Format("2006-01-02 15:04:05.000"))
	tempStr += fmt.Sprintf("%s", args...)
	fmt.Printf("\u001B[1;%dm%s\u001B[0m", 31, tempStr)
}

func logLnError(args ...interface{}) {
	tempStr := fmt.Sprintf(" [%s]: [%s] - ", "ERROR", time.Now().Format("2006-01-02 15:04:05.000"))
	tempStr += fmt.Sprintf("%s", args...)
	fmt.Printf("\u001B[1;%dm%s\u001B[0m\n", 31, tempStr)
}
