package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
)

const PARALLEL = 20

func main() {
	// 并发创建store与index测试
	group.Add(PARALLEL)
	for i := 0; i < PARALLEL; i += 1 {
		go createStore(i, util.ProjectName, util.LogStoreName)
	}
	group.Wait()
	// time.Sleep(30 * time.Minute)
	logstore, err := util.Client.GetLogStore(util.ProjectName, util.LogStoreName)
	if err != nil {
		panic(err)
	}
	fmt.Printf(
		"create logstore successfully: %s %v\n",
		logstore.Name,
		time.Unix(int64(logstore.CreateTime), 0).Format("2006-01-02 15:04:05"),
	)
	time.Sleep(30 * time.Minute)
	updateLogstore := &sls.LogStore{
		Name:        util.LogStoreName,
		TTL:         2,
		ShardCount:  10,
		AutoSplit:   false,
		WebTracking: true,
	}
	err = util.Client.UpdateLogStoreV2(util.ProjectName, updateLogstore)
	if err != nil {
		panic(err)
	}
	fmt.Println("update logstore suecessed")
	fmt.Println("Prepare to delete the logstore after 30 seconds")
	time.Sleep(30 * time.Second)
	err = util.Client.DeleteLogStore(util.ProjectName, util.LogStoreName)
	if err != nil {
		panic(err)
	}
	fmt.Println("Delete Logstore successfully")
}

var (
	group = &sync.WaitGroup{}
)

func createStore(gid int, proj, store string) {
	defer group.Done()
	fmt.Printf("git -> %d\n", gid)
	err := util.Client.CreateLogStore(proj, store, 2, 2, true, 64)
	if err != nil {
		if (err.(*sls.Error)).Code != "LogStoreAlreadyExist" {
			panic(err)
		}
		return
	}
	if err := createIndex(proj, store); err != nil {
		panic(err)
	}
}

func createIndex(proj, store string) error {
	return util.Client.CreateIndex(
		proj, store,
		sls.Index{
			// 指定字段索引信息
			Keys: map[string]sls.IndexKey{
				"@context": sls.IndexKey{
					Type:     "json",
					Token:    []string{",", "'", "\"", "(", ")", "[", "]", "?", ":", " ", "-"}, // 分词符
					Chn:      true,
					DocValue: true,
					JsonKeys: map[string]*sls.JsonKey{
						"logSize": &sls.JsonKey{
							Type:     "long",
							DocValue: true,
						},
						"metadata.group": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"metadata.timestamp": &sls.JsonKey{ // 第二级及后续级别必须扁平化
							Type:     "text",
							DocValue: true,
						},
						"offset": &sls.JsonKey{ // 第二级及后续级别必须扁平化
							Type:     "long",
							DocValue: true,
						},
						"partition": &sls.JsonKey{ // 第二级及后续级别必须扁平化
							Type:     "long",
							DocValue: true,
						},
						"rawSize": &sls.JsonKey{
							Type:     "long",
							DocValue: true,
						},
						"timestamp": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"topic": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"list": &sls.JsonKey{
							Type:     "text", // 数组类型值转换为字符串形式
							DocValue: true,
						},
					},
				},
				"@metadata": sls.IndexKey{
					Type:     "json",
					Token:    []string{",", "'", "\"", "(", ")", "[", "]", "?", ":", " ", "-"}, // 分词符
					Chn:      true,
					DocValue: true,
					JsonKeys: map[string]*sls.JsonKey{
						"beat": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"topic": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"type": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"version": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
					},
				},
				"kubernetes": sls.IndexKey{
					Type:     "json",
					Token:    []string{",", "'", "\"", "(", ")", "[", "]", "?", ":", " ", "-"}, // 分词符
					Chn:      true,
					DocValue: true,
					JsonKeys: map[string]*sls.JsonKey{
						"annotations.helm_sh/namespace": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"annotations.helm_sh/release": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"container_name": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"labels.controller_caicloud_io/chart": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"labels.project": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"namespace_name": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"pod_name": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
					},
				},
				"@timestamp": sls.IndexKey{
					Type:     "text",
					Token:    []string{}, // 不能为nil空
					DocValue: true,
				},
				"_id": sls.IndexKey{
					Type:     "text",
					Token:    []string{}, // 不能为nil空
					DocValue: true,
				},
				"cluster": sls.IndexKey{
					Type:     "text",
					Token:    []string{}, // 不能为nil空
					DocValue: true,
				},
				"log": sls.IndexKey{
					Type:     "text",
					Token:    []string{",", "'", "\"", "(", ")", "[", "]", "?", ":", " ", "-"}, // 分词符
					DocValue: true,
				},
				"node_name": sls.IndexKey{
					Type:     "text",
					Token:    []string{}, // 不能为nil空
					DocValue: true,
				},
				"offset": sls.IndexKey{
					Type:     "long",
					DocValue: true,
				},
				"stream": sls.IndexKey{
					Type:     "text",
					Token:    []string{}, // 不能为nil空
					DocValue: true,
				},
			},
			// 对未配制索引的字段生效
			Line: &sls.IndexLine{
				Token:         []string{",", ":", ";", "|"}, // 分词符
				CaseSensitive: true,
				IncludeKeys:   []string{},
				ExcludeKeys:   []string{},
			},
		},
	)
}
