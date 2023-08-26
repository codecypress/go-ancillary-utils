package logging

import (
	"fmt"
	"github.com/codecypress/go-ancillary-utils/miscellaneous"
	"github.com/sirupsen/logrus"
	"runtime"
	"strings"
)

type myFormatter struct {
	logrus.TextFormatter
}

func (f *myFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	/*loggingFormat, err := xmlutils.ConfGetLoggingFormat()
	if err != nil {
		loggingFormat = "FORMAT B"
	}*/

	loggingFormat := "FORMAT B"

	switch loggingFormat {
	case "FORMAT A":
		{
			return []byte(fmt.Sprintf(" [%s]: [%s] - %s\n", strings.ToUpper(entry.Level.String()), entry.Time.Format(f.TimestampFormat), entry.Message)), nil
		}
	case "FORMAT B":
		{
			tempStr := fmt.Sprintf(" [%s]: [%s] - ", strings.ToUpper(entry.Level.String()), entry.Time.Format(f.TimestampFormat))
			_, file, no, ok := runtime.Caller(1)
			if ok {
				tempStr += fmt.Sprintf("%s Line: %d - ", miscellaneous.GetCallerFilename(file), no)
			}
			tempStr += fmt.Sprintf("%s", entry.Message)
			return []byte(fmt.Sprintf("%s\n", tempStr)), nil
		}
	case "FORMAT C":
		{
			// this whole mess of dealing with ansi color codes is required if you want the colored output otherwise you will lose colors in the log levels
			var levelColor int
			switch entry.Level {
			case logrus.DebugLevel, logrus.TraceLevel:
				levelColor = 31 // gray
			case logrus.WarnLevel:
				levelColor = 33 // yellow
			case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
				levelColor = 31 // red
			default:
				levelColor = 36 // blue
			}
			//return []byte(fmt.Sprintf("[%s] - \x1b[%dm%s\x1b[0m - %s\n", entry.Time.Format(f.TimestampFormat), levelColor, strings.ToUpper(entry.Level.String()), entry.Message)), nil
			return []byte(fmt.Sprintf(" [\u001B[%dm%s\u001B[0m]: [%s] - %s\n", levelColor, strings.ToUpper(entry.Level.String()), entry.Time.Format(f.TimestampFormat), entry.Message)), nil

		}
	case "FORMAT D":
		{
			// this whole mess of dealing with ansi color codes is required if you want the colored output otherwise you will lose colors in the log levels
			var levelColor int
			switch entry.Level {
			case logrus.DebugLevel, logrus.TraceLevel:
				levelColor = 31 // gray
			case logrus.WarnLevel:
				levelColor = 33 // yellow
			case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
				levelColor = 31 // red
			default:
				levelColor = 36 // blue
			}

			tempStr := fmt.Sprintf(" [%s]: [%s] - ", strings.ToUpper(entry.Level.String()), entry.Time.Format(f.TimestampFormat))
			_, file, no, ok := runtime.Caller(1)
			if ok {
				tempStr += fmt.Sprintf("%s Line: %d - ", miscellaneous.GetCallerFilename(file), no)
			}
			tempStr += fmt.Sprintf("%s", entry.Message)

			return []byte(fmt.Sprintf("\u001B[%dm%s\u001B[0m\n", levelColor, tempStr)), nil
		}

	default:
		{
			// this whole mess of dealing with ansi color codes is required if you want the colored output otherwise you will lose colors in the log levels
			var levelColor int
			switch entry.Level {
			case logrus.DebugLevel, logrus.TraceLevel:
				levelColor = 31 // gray
			case logrus.WarnLevel:
				levelColor = 33 // yellow
			case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
				levelColor = 31 // red
			default:
				levelColor = 36 // blue
			}
			//return []byte(fmt.Sprintf("[%s] - \x1b[%dm%s\x1b[0m - %s\n", entry.Time.Format(f.TimestampFormat), levelColor, strings.ToUpper(entry.Level.String()), entry.Message)), nil
			return []byte(fmt.Sprintf(" [\u001B[%dm%s\u001B[0m]: [%s] - %s\n", levelColor, strings.ToUpper(entry.Level.String()), entry.Time.Format(f.TimestampFormat), entry.Message)), nil
		}
	}
}

/*func newLog() *logrus.Logger {

	/*customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	logrus.SetFormatter(customFormatter)
	logrus.Info("Hello Walrus before FullTimestamp=true")
	customFormatter.FullTimestamp = true
	logrus.Info("Hello Walrus after FullTimestamp=true")
}
*/

func GetCustomFormatter() *myFormatter {
	return &myFormatter{
		logrus.TextFormatter{
			FullTimestamp:          true,
			TimestampFormat:        "2006-01-02 15:04:05.000",
			ForceColors:            true,
			DisableLevelTruncation: true,
		},
	}
}
