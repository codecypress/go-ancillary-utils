package cypressutils

import "strings"

const (
	yyyy = "2006"
	yy   = "06"
	mmmm = "January"
	mmm  = "Jan"
	mm   = "01"
	dddd = "Monday"
	ddd  = "Mon"
	dd   = "02"

	HHT = "03"
	HH  = "15"
	MM  = "04"
	SS  = "05"
	ss  = "05"
	tt  = "PM"
	Z   = "MST"
	ZZZ = "MST"

	o = "Z07:00"
)

func GetStandardFormat(format string) string {
	var standardFormat = format
	if strings.Contains(standardFormat, "YYYY") {
		standardFormat = strings.Replace(standardFormat, "YYYY", yyyy, -1)
	} else if strings.Contains(standardFormat, "yyyy") {
		standardFormat = strings.Replace(standardFormat, "yyyy", yyyy, -1)
	} else if strings.Contains(standardFormat, "YY") {
		standardFormat = strings.Replace(standardFormat, "YY", yy, -1)
	} else if strings.Contains(standardFormat, "yy") {
		standardFormat = strings.Replace(standardFormat, "yy", yy, -1)
	}

	if strings.Contains(standardFormat, "MMMM") {
		standardFormat = strings.Replace(standardFormat, "MMMM", mmmm, -1)
	} else if strings.Contains(standardFormat, "mmmm") {
		standardFormat = strings.Replace(standardFormat, "mmmm", mmmm, -1)
	} else if strings.Contains(standardFormat, "MMM") {
		standardFormat = strings.Replace(standardFormat, "MMM", mmm, -1)
	} else if strings.Contains(standardFormat, "mmm") {
		standardFormat = strings.Replace(standardFormat, "mmm", mmm, -1)
	} else if strings.Contains(standardFormat, "mm") {
		standardFormat = strings.Replace(standardFormat, "mm", mm, -1)
	}

	if strings.Contains(standardFormat, "dddd") {
		standardFormat = strings.Replace(standardFormat, "dddd", dddd, -1)
	} else if strings.Contains(standardFormat, "ddd") {
		standardFormat = strings.Replace(standardFormat, "ddd", ddd, -1)
	} else if strings.Contains(standardFormat, "dd") {
		standardFormat = strings.Replace(standardFormat, "dd", dd, -1)
	}

	if strings.Contains(standardFormat, "tt") {
		if strings.Contains(standardFormat, "HH") {
			standardFormat = strings.Replace(standardFormat, "HH", HHT, -1)
		} else if strings.Contains(standardFormat, "hh") {
			standardFormat = strings.Replace(standardFormat, "hh", HHT, -1)
		}
		standardFormat = strings.Replace(standardFormat, "tt", tt, -1)
	} else {
		if strings.Contains(standardFormat, "HH") {
			standardFormat = strings.Replace(standardFormat, "HH", HH, -1)
		} else if strings.Contains(standardFormat, "hh") {
			standardFormat = strings.Replace(standardFormat, "hh", HH, -1)
		}
		standardFormat = strings.Replace(standardFormat, "tt", "", -1)
	}

	if strings.Contains(standardFormat, "MM") {
		standardFormat = strings.Replace(standardFormat, "MM", MM, -1)
	}

	if strings.Contains(standardFormat, "SS") {
		standardFormat = strings.Replace(standardFormat, "SS", SS, -1)
	} else if strings.Contains(standardFormat, "ss") {
		standardFormat = strings.Replace(standardFormat, "ss", SS, -1)
	}

	if strings.Contains(standardFormat, "ZZZ") {
		standardFormat = strings.Replace(standardFormat, "ZZZ", ZZZ, -1)
	} else if strings.Contains(standardFormat, "zzz") {
		standardFormat = strings.Replace(standardFormat, "zzz", ZZZ, -1)
	} else if strings.Contains(standardFormat, "Z") {
		standardFormat = strings.Replace(standardFormat, "Z", Z, -1)
	} else if strings.Contains(standardFormat, "z") {
		standardFormat = strings.Replace(standardFormat, "z", Z, -1)
	}

	if strings.Contains(standardFormat, "tt") {
		standardFormat = strings.Replace(standardFormat, "tt", tt, -1)
	}
	if strings.Contains(standardFormat, "o") {
		standardFormat = strings.Replace(standardFormat, "o", o, -1)
	}
	return standardFormat
}
