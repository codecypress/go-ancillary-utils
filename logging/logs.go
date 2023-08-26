package logging

import (
	"fmt"
	"github.com/codecypress/go-ancillary-utils/datetime"
	"runtime"
	"strings"
)

func LogRequest(args ...interface{}) {
	fmt.Printf(" [INFO]: [%s] -", datetime.GetCurrentDateTime(datetime.DATETIME_WITH_MILLI_FORMAT))

	shouldPrintHyphen := false

	for _, arg := range args {
		if shouldPrintHyphen {
			fmt.Print("- ")
		} else {
			shouldPrintHyphen = true
		}
		fmt.Printf("%v", arg)
	}

	fmt.Println()

	//INFO [thread-XNIO-1 task-1] - 2021-08-02 16:04:07.409; - HandlerBaseLayer.printRequestInfo() - Request URI: /api/rest/o/token
}

func Println(args ...interface{}) {
	logln("INFO", true, 36, args)
}

func Printf(format string, args ...interface{}) {
	logf(format, "INFO", true, 36, args)
}

func PrintErrorln(args ...interface{}) {
	logln("ERROR", true, 31, args)
}

func PrintErrorf(format string, args ...interface{}) {
	logf(format, "ERROR", true, 31, args)
}

func PrintWarningln(args ...interface{}) {
	logln("WARNING", true, 33, args)
}

func PrintWarningf(format string, args ...interface{}) {
	logf(format, "WARNING", true, 33, args)
}

func PrintFatalln(args ...interface{}) {
	logln("FATAL", true, 31, args)
}

func PrintFatalf(format string, args ...interface{}) {
	logf(format, "FATAL", true, 31, args)
}

func PrintlnNoColor(args ...interface{}) {
	logln("INFO", false, 36, args)
}

func PrintfNoColor(format string, args ...interface{}) {
	logf(format, "INFO", false, 36, args)
}

func PrintErrorlnNoColor(args ...interface{}) {
	logln("ERROR", false, 31, args)
}

func PrintErrorfNoColor(format string, args ...interface{}) {
	logf(format, "ERROR", false, 31, args)
}

func PrintWarninglnNoColor(args ...interface{}) {
	logln("WARNING", false, 33, args)
}

func PrintWarningfNoColor(format string, args ...interface{}) {
	logf(format, "WARNING", false, 33, args)
}

func PrintFatallnNoColor(args ...interface{}) {
	logln("FATAL", false, 31, args)
}

func PrintFatalfNoColor(format string, args ...interface{}) {
	logf(format, "FATAL", false, 31, args)
}

func logln(logLevel string, applyColor bool, color int, args ...interface{}) {
	tempStr := fmt.Sprintf(" [%s]: [%s] - ", logLevel, datetime.GetCurrentDateTime(datetime.DATETIME_WITH_MILLI_FORMAT))

	_, file, no, ok := runtime.Caller(1)
	if ok {
		tempStr += fmt.Sprintf("%s Line: %d - ", getFilename(file), no)
	}

	tempStr += fmt.Sprintf("%s", args...)

	if applyColor {
		fmt.Printf("\u001B[1;%dm%s\u001B[0m\n", color, tempStr)
	} else {
		fmt.Printf("%s\n", tempStr)
	}
}

func logf(format, logLevel string, applyColor bool, color int, args ...interface{}) {
	tempStr := fmt.Sprintf("  [%s]: [%s] - ", logLevel, datetime.GetCurrentDateTime(datetime.DATETIME_WITH_MILLI_FORMAT))

	_, file, no, ok := runtime.Caller(1)
	if ok {
		tempStr += fmt.Sprintf("%s Line: %d - ", getFilename(file), no)
	}

	tempStr += fmt.Sprintf("%s", args...)

	if applyColor {
		fmt.Printf("\u001B[1;%dm"+format+"\u001B[0m", color, tempStr)
	} else {
		fmt.Printf(format, tempStr)
	}
}

/*func CallerFileAndLineNo() string{
	_, file, no, ok := runtime.Caller(1)
	if ok {
		return fmt.Sprintf("%s Line: %d - ", getFilename(file), no)
	} else{
		return ""
	}
}*/

func getFilename(filePath string) string {
	if strings.Contains(filePath, "/") {
		return filePath[strings.LastIndex(filePath, "/")+1:]
	} else {
		return filePath[strings.LastIndex(filePath, "\\")+1:]
	}
}
