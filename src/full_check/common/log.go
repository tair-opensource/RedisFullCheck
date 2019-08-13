package common

import (
	"github.com/cihub/seelog"
)

func InitLog(logFile string, logLevel string) (seelog.LoggerInterface, error) {
	var logConfig string
	if len(logFile) == 0 {
		logConfig = `
			<seelog minlevel="debug">
				<outputs formatid="main">
					<filter levels="` + logLevel + `">
                        <console />
                    </filter>
				</outputs>
				<formats>
					<format id="main" format="[%LEVEL %Date-%Time %File:%Line]: %Msg%n"/>
				</formats>
			</seelog>`
	} else {
		logConfig = `
			<seelog minlevel="debug">
				<outputs formatid="main">
					<filter levels="` + logLevel + `">
						<file path="` + logFile + `"/>
					</filter>
				</outputs>
				<formats>
					<format id="main" format="[%LEVEL %Date-%Time %File:%Line]: %Msg%n"/>
				</formats>
			</seelog>`
	}
	return seelog.LoggerFromConfigAsBytes([]byte(logConfig))
}
