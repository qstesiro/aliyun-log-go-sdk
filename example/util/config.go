package util

import (
	"os"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

// When you use the file under example, please configure the required variables here.
// Project define Project for test
var (
	ProjectName     = "ProjectName"
	Endpoint        = "Endpoint"
	LogStoreName    = "LogStoreName"
	AccessKeyID     = "AccessKeyID"
	AccessKeySecret = "AccessKeySecret"
	Client          sls.ClientInterface
)

const (
	ACCESS_KEY_ID     = "ACCESS_KEY_ID"
	ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET"
	ENDPOINT          = "ENDPOINT"
)

// You can get the variable from the environment variable, or fill in the required configuration directly in the init function.
func init() {
	ProjectName = "hmm-demo-1"
	AccessKeyID = os.Getenv(ACCESS_KEY_ID)
	AccessKeySecret = os.Getenv(ACCESS_KEY_SECRET)
	Endpoint = os.Getenv(ENDPOINT) // just like cn-hangzhou.log.aliyuncs.com
	// LogStoreName = "store-01"
	LogStoreName = "store-02"

	Client = sls.CreateNormalInterface(Endpoint, AccessKeyID, AccessKeySecret, "")
}
