package nodesmigrator

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

func logInfo(log string) {
	fmt.Println(fmt.Sprintf("INFO: %s", log))
	logrus.Info(log)
}

func logWarn(log string) {
	fmt.Println(fmt.Sprintf("WARN: %s", log))
	logrus.Warn(log)
}

func logDebug(log string) {
	logrus.Debugf(log)
}

func logPanic(log string) {
	fmt.Println(log)
	logrus.Panic(log)
}
