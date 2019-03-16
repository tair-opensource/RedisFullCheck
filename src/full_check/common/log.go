package common

import (
	"github.com/cihub/seelog"
)

func InitLog(logFile string) (seelog.LoggerInterface, error) {
	var logConfig string
	if len(logFile) == 0 {
		logConfig = `
			<seelog>
				<outputs formatid="main">
					<console />
				</outputs>
				<formats>
					<format id="main" format="%LEVEL %Date-%Time] (%File:%Line): %Msg%n"/>
				</formats>
			</seelog>`
	} else {
		logConfig = `
			<seelog>
				<outputs formatid="main">
					<file path="` + logFile + `"/>
				</outputs>
				<formats>
					<format id="main" format="%LEVEL %Date-%Time] (%File:%Line): %Msg%n"/>
				</formats>
			</seelog>`
	}
	return seelog.LoggerFromConfigAsBytes([]byte(logConfig))
}
