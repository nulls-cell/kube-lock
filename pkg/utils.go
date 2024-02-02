package pkg

import (
	stackerror "github.com/nulls-cell/stackerror/pkg/error"
	"k8s.io/apimachinery/pkg/util/uuid"
	"os"
)

var ProcessUuid string

func init() {
	ProcessUuid = string(uuid.NewUUID())
}

func GetHostIdentity() (string, stackerror.IStackError) {
	hostName, err := os.Hostname()
	if err != nil {
		return "", stackerror.WrapStackError(err)
	}
	return hostName, nil
}

func GetProcessIdentity() string {
	return ProcessUuid
}

func GetInstanceIdentity() string {
	return string(uuid.NewUUID())
}
