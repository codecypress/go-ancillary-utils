package datetime

import (
	"bytes"
	"fmt"
	"github.com/codecypress/go-ancillary-utils/exceptions"
	cErrors "github.com/pkg/errors"
	"time"
)

const DATE_FORMAT = "2006-01-02"
const DATETIME_FORMAT = "2006-01-02 15:04:05"
const DATETIME_WITH_MILLI_FORMAT = "2006-01-02 15:04:05.000"
const DATETIME_WITH_MICRO_FORMAT = "2006-01-02 15:04:05.000000"
const DATETIME_WITH_NANO_FORMAT = "2006-01-02 15:04:05.000000000"

func GetCurrentDate(format ...string) string {
	tempFormat := "2006-01-02"
	if format != nil {
		tempFormat = format[0]
	}
	return time.Now().Format(tempFormat)
}

/*func GetCurrentDateUTC(format ...string) string {
	tempFormat := "2006-01-02"
	if format != nil {
		tempFormat = format[0]
	}
	return time.Now().UTC().Format(tempFormat)
}*/

func GetCurrentDateTime(format ...string) string {
	tempFormat := "2006-01-02 15:04:05"
	if format != nil {
		tempFormat = format[0]
	}
	return time.Now().Format(tempFormat)
}

/*func GetCurrentDateTimeUTC(format ...string) string {
	tempFormat := "2006-01-02 15:04:05"
	if format != nil {
		tempFormat = format[0]
	}

	return time.Now().UTC().Format(tempFormat)
}*/

func GetCurrentDateTimeAsTimeFacadeUTC(format ...string) time.Time {
	tempFormat := "2006-01-02 15:04:05"
	if format != nil {
		tempFormat = format[0]
	}

	currTime := GetCurrentDateTime(tempFormat)
	return FormatStringToDateFacadeUTC(tempFormat, currTime)

	/*t, err := time.Parse(tempFormat, GetCurrentDateTime(tempFormat))
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return time.Time{}
	}
	return t*/
}

/*func GetCurrentDateTimeAsTimeUTC(format ...string) time.Time {
	tempFormat := "2006-01-02 15:04:05"
	if format != nil {
		tempFormat = format[0]
	}
	return FormatStringToDateUTC(tempFormat, time.Now().UTC().Format(tempFormat))
}*/

/*func GetCurrentDateTimeWithMillis(format ...string) string {
	tempFormat := "2006-01-02 15:04:05.000"
	if format != nil {
		tempFormat = format[0]
	}
	return time.Now().Local().Format(tempFormat)
}

func GetCurrentDateTimeWithMillisUTC(format ...string) string {
	tempFormat := "2006-01-02 15:04:05.000"
	if format != nil {
		tempFormat = format[0]
	}
	return time.Now().UTC().Format(tempFormat)
}*/

func GetCurrentUnixTimestampFacadeUTC() int64 {
	t := GetCurrentDateTimeAsTimeFacadeUTC(DATETIME_WITH_NANO_FORMAT)
	return t.UnixNano() / int64(1e6)
}

/*func GetCurrentUnixTimestampUTC() int64 {
	return time.Now().UTC().UnixNano() / int64(1e6)
}*/

//WILL always Return the date very well, but pretends its UTC

func FormatStringToDateFacadeUTC(layout, strDate string) time.Time {
	t, err := time.Parse(layout, strDate)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return time.Time{}
	}

	return t
}

/*func FormatStringToDateUTC(layout, strDate string) time.Time {
	t, err := time.Parse(layout, strDate)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return time.Time{}
	}
	return t.UTC()
}*/

func FormatStringToDateToString(originalLayout, strOriginalDate, strNewLayout string) string {
	t, err := time.Parse(originalLayout, strOriginalDate)
	if err != nil {

		exceptions.ThrowException(cErrors.Cause(err))
		return ""
	}
	return t.Format(strNewLayout)
}

/*func FormatStringToDateToStringUTC(originalLayout, strOriginalDate, strNewLayout string) string {
	t, err := time.Parse(originalLayout, strOriginalDate)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return ""
	}
	return t.UTC().Format(strNewLayout)
}*/

func FormatDateToString(layout string, time time.Time) string {
	return time.Format(layout)
}

/*func FormatDateToStringUTC(layout string, time time.Time) string {
	return time.UTC().Format(layout)
}*/

func ConvertDateTimeToMillisecondsFacadeUTC(layout string, strDateTime string) int64 {
	t := FormatStringToDateFacadeUTC(layout, strDateTime)
	return t.UnixNano() / 1000000
}

/*func ConvertDateTimeToMillisecondsUTC(layout string, strDateTime string) int64 {
	t, err := time.Parse(layout, strDateTime)

	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return 0
	}

	return t.UTC().UnixNano() / 1000000
}*/

/*func ParseMilliTimestamp(layout string, millis int64) time.Time {
	sec := millis / 1000
	msec := millis % 1000

	t := time.Unix(sec, msec*int64(time.Millisecond))



	str := t.Format(layout)

	t, _ = time.Parse(layout, str)

	return t
}*/

func ParseMilliTimestampFacadeUTC(layout string, millis int64, useActualUTC bool) time.Time {
	sec := millis / 1000
	msec := millis % 1000

	t := time.Unix(sec, msec*int64(time.Millisecond))
	str := t.Format(layout)
	t, _ = time.Parse(layout, str)

	return t
}

func ConvertToSeconds(period int, periodUnit string) int {
	converted := period

	switch periodUnit {
	case "SECOND":
		{
			converted = period
			break
		}

	case "MINUTE":
		{
			converted = period * 60
			break
		}

	case "HOUR":
		{
			converted = period * 60 * 60
			break
		}

	case "DAY":
		{
			converted = period * 60 * 60 * 24
			break
		}

	case "WEEK":
		{
			converted = period * 60 * 60 * 24 * 7
			break
		}

	case "MONTH":
		{
			converted = period * 60 * 60 * 24 * 7 * 30
			break
		}

	case "YEAR":
		{
			converted = period * 60 * 60 * 24 * 7 * 30 * 12
			break
		}
	}
	return converted
}

func GetDelay(periodUnit string, expiry int64) int64 {
	switch periodUnit {
	case "MINUTES":
		return expiry * 1000 * 60
	case "HOURS":
		return expiry * 1000 * 60 * 60
	case "DAYS":
		return expiry * 1000 * 60 * 60 * 24
	default: /* SECONDS */
		return expiry * 1000
	}
}

func MillisToLongDHMS(duration int64) string {
	ONE_SECOND := int64(1000)
	//SECONDS := 60

	ONE_MINUTE := ONE_SECOND * 60
	//MINUTES := 60

	ONE_HOUR := ONE_MINUTE * 60
	//HOURS := 24

	ONE_DAY := ONE_HOUR * 24

	var res bytes.Buffer
	temp := int64(0)
	hasDay := false
	hasHasHour := false
	hasMinute := false

	if duration >= ONE_SECOND {
		temp = duration / ONE_DAY
		if temp > 0 {
			hasDay = true
			duration -= temp * ONE_DAY

			res.WriteString(fmt.Sprintf("%v", temp))
			res.WriteString(" day")
			if temp > 1 {
				res.WriteString("s")
			} else {
				res.WriteString("")
			}

			if duration >= ONE_MINUTE {
				res.WriteString(", ")
			} else {
				res.WriteString("")
			}
		}

		temp = duration / ONE_HOUR
		if temp > 0 {
			hasHasHour = true
			duration -= temp * ONE_HOUR

			res.WriteString(fmt.Sprintf("%v", temp))
			res.WriteString(" hour")
			if temp > 1 {
				res.WriteString("s")
			} else {
				res.WriteString("")
			}

			if duration >= ONE_MINUTE {
				res.WriteString(", ")
			} else {
				res.WriteString("")
			}
		}

		if !hasDay {
			temp = duration / ONE_MINUTE
			if temp > 0 {
				hasMinute = true
				duration -= temp * ONE_MINUTE

				res.WriteString(fmt.Sprintf("%v", temp))
				res.WriteString(" minute")
				if temp > 1 {
					res.WriteString("s")
				} else {
					res.WriteString("")
				}
			}

			if !hasHasHour && !hasMinute {
				res.WriteString("1 minute")
				/*if(!hasHasHour){
				    //temp = duration / ONE_SECOND;

				}*/
			}
		}
		return res.String()
	} else {
		return "0 seconds"
	}
}
