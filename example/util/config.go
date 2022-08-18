package util

import (
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

// You can get the variable from the environment variable, or fill in the required configuration directly in the init function.
func init() {
	ProjectName = "hmm-demo-1"
	AccessKeyID = "LTAI5tDQ776Zcd3aWUkKeHkT"
	AccessKeySecret = "1nzAq9CJB2XLWmPifLzd0VhhGbXV0B"
	Endpoint = "cn-qingdao.log.aliyuncs.com" // just like cn-hangzhou.log.aliyuncs.com
	// LogStoreName = "store-01"
	LogStoreName = "store-02"

	Client = sls.CreateNormalInterface(Endpoint, AccessKeyID, AccessKeySecret, "")
}
