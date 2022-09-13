package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/gogo/protobuf/proto"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
)

const (
	WAITING = 15
)

func main() {
	fmt.Println("loghub sample begin")
	logstore_name := "test"
	fmt.Printf("CreateStore project: %s, store: %s\n", util.ProjectName, logstore_name)
	if err := CreateStore(util.ProjectName, logstore_name); err != nil {
		fmt.Printf("CreateStore fail, err: %s\n", err)
		return
	}
	fmt.Printf("CreateIndex project: %s, store: %s\n", util.ProjectName, logstore_name)
	if err := CreateIndex(util.ProjectName, logstore_name); err != nil {
		fmt.Printf("CreateIndex fail, err: %s\n", err)
		return
	}
	// 创建索引后如果不等待30秒数据写入虽然返回成功,但实际是失败的 ???
	fmt.Printf("waiting %d seconds ...\n", WAITING*3)
	time.Sleep(WAITING * 3 * time.Second) // 为什么等待 ???
	beginTime := time.Now().Unix()
	fmt.Printf("PutLogs project: %s, store: %s\n", util.ProjectName, logstore_name)
	if err := PutLogs(util.ProjectName, logstore_name); err != nil {
		fmt.Printf("PutLogs fail, err: %s\n", err)
		return
	}
	endTime := time.Now().Unix()
	fmt.Printf("waiting %d seconds ...\n", WAITING)
	time.Sleep(WAITING * time.Second) // 为什么等待 ???
	if err := getLogs(util.ProjectName, logstore_name, beginTime, endTime); err != nil {
		fmt.Printf("PutLogs fail, err: %s\n", err)
		return
	}
	fmt.Println("index sample end")
}

func CreateStore(proj, store string) error {
	util.Client.DeleteLogStore(proj, store)
	fmt.Printf("waiting %d seconds ...\n", WAITING)
	time.Sleep(WAITING * time.Second) // 为什么等待 ???
	err := util.Client.CreateLogStore(proj, store, 90, 2, true, 16)
	if err != nil {
		return err
	}
	// fmt.Printf("waiting %d seconds ...\n", WAITING)
	// time.Sleep(WAITING * time.Second) // 为什么等待 ???
	// logstore, err := util.Client.GetLogStore(proj, store)
	// if err != nil {
	// 	return err
	// }
	// fmt.Printf("GetLogStore success, name: %s, ttl: %d, shardCount: %d, createTime: %d, lastModifyTime: %d\n", logstore.Name, logstore.TTL, logstore.ShardCount, logstore.CreateTime, logstore.LastModifyTime)
	return nil
}

// func CreateIndex(proj, store string) error {
// 	return util.Client.CreateIndex(
// 		proj, store,
// 		sls.Index{
// 			// 指定字段索引信息
// 			Keys: map[string]sls.IndexKey{
// 				// JsonKey
// 				"col_0": {
// 					Type:          "json",
// 					Alias:         "col0",
// 					CaseSensitive: false,
// 					Token:         []string{",", ":", " ", "-"}, // 分词符
// 					Chn:           true,
// 					DocValue:      true,
// 					JsonKeys: map[string]*sls.JsonKey{
// 						"k0": &sls.JsonKey{
// 							Type:     "text",
// 							DocValue: true,
// 						},
// 						"k1": &sls.JsonKey{
// 							Type:     "long",
// 							DocValue: true,
// 						},
// 						"k2.k0.k0": &sls.JsonKey{ // 第二级及后续级别必须扁平化
// 							Type:     "text",
// 							DocValue: true,
// 						},
// 						"k2.k0.k1": &sls.JsonKey{ // 第二级及后续级别必须扁平化
// 							Type:     "long",
// 							DocValue: true,
// 						},
// 						"k2.k1": &sls.JsonKey{ // 第二级及后续级别必须扁平化
// 							Type:     "long",
// 							DocValue: true,
// 						},
// 					},
// 				},
// 				// 展开Json
// 				// "col_0.key_0": {
// 				// 	Type:          "text",
// 				// 	Alias:         "col0.key0",
// 				// 	CaseSensitive: false,
// 				// 	Token:         []string{",", ":", " "}, // 分词符
// 				// 	Chn:           true,
// 				// 	DocValue:      true,
// 				// },
// 				// "col_0.key_1": {
// 				// 	Type:     "long",
// 				// 	DocValue: true,
// 				// },
// 				"col_1": {
// 					Type:     "text",
// 					Token:    []string{",", ":", " ", "-"}, // 分词符
// 					DocValue: true,
// 				},
// 			},
// 			// 对未配制索引的字段生效
// 			Line: &sls.IndexLine{
// 				Token:         []string{",", ":", ";", "|"}, // 分词符
// 				CaseSensitive: true,
// 				IncludeKeys:   []string{},
// 				ExcludeKeys:   []string{},
// 			},
// 		},
// 	)
// }

func CreateIndex(proj, store string) error {
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

// func PutLogs(proj, store string) error {
// 	begin_time := uint32(time.Now().Unix())
// 	rand.Seed(int64(begin_time))
// 	// put logs to logstore
// 	// 写入10组每组100条每条10个字段,只有col_0/1有索引项
// 	// 数据写入不稳定会少数据(因为是通过公网) ???
// 	groupMax, logMax := 1, 10
// 	for loggroupIdx := 0; loggroupIdx < groupMax; loggroupIdx++ {
// 		logs := []*sls.Log{}
// 		for logIdx := 0; logIdx < logMax; logIdx++ {
// 			content := []*sls.LogContent{}
// 			for colIdx := 0; colIdx < 10; colIdx++ {
// 				if colIdx == 0 {
// 					// JsonKey
// 					content = append(
// 						content,
// 						&sls.LogContent{
// 							Key: proto.String("col_0"),
// 							Value: proto.String(
// 								fmt.Sprintf(
// 									`{
//                                        "k0": "text-%d",
//                                        "k1": %d,
//                                        "k2": {
//                                          "k0": %d,
//                                          "k1": {
//                                            "k0": "text-%d",
//                                            "k1": %d
//                                          }
//                                        }
//                                      }`,
// 									rand.Intn(1000),
// 									rand.Intn(1000),
// 									rand.Intn(1000),
// 									rand.Intn(1000),
// 									rand.Intn(1000),
// 								),
// 							),
// 						},
// 					)
// 				} else {
// 					content = append(content, &sls.LogContent{
// 						Key:   proto.String(fmt.Sprintf("col_%d", colIdx)),
// 						Value: proto.String(fmt.Sprintf("loggroup idx: %d, log idx: %d, col idx: %d, value: %d", loggroupIdx, logIdx, colIdx, rand.Intn(10000000))),
// 					})
// 				}
// 			}
// 			log := &sls.Log{
// 				Time:     proto.Uint32(uint32(time.Now().Unix())),
// 				Contents: content,
// 			}
// 			logs = append(logs, log)
// 		}
// 		loggroup := &sls.LogGroup{
// 			Logs:     logs,
// 			Topic:    proto.String("demo-topic"),
// 			Category: proto.String("demo-categroy"),
// 			Source:   proto.String("10.230.201.117"),
// 			LogTags: []*sls.LogTag{
// 				&sls.LogTag{
// 					Key:   proto.String("t1"),
// 					Value: proto.String("v1"),
// 				},
// 				&sls.LogTag{
// 					Key:   proto.String("t2"),
// 					Value: proto.String("v2"),
// 				},
// 			},
// 		}
// 		// PutLogs API Ref: https://intl.aliyun.com/help/doc-detail/29026.htm
// 		if err := util.Client.PutLogs(proj, store, loggroup); err != nil {
// 			fmt.Printf("PutLogs fail, err: %s\n", err)
// 		}
// 		fmt.Printf("#%d waiting %d seconds ...\n", loggroupIdx, 1)
// 		time.Sleep(time.Second) // 为什么等待 ???
// 	}
// 	return nil
// }

func PutLogs(proj, store string) error {
	// put logs to logstore
	groupMax, logMax := 10, 100
	for loggroupIdx := 0; loggroupIdx < groupMax; loggroupIdx++ {
		logs := []*sls.Log{}
		for logIdx := 0; logIdx < logMax; logIdx++ {
			logs = append(logs, toLog(getMap()))
		}
		loggroup := &sls.LogGroup{
			Logs:     logs,
			Topic:    proto.String("demo-topic"),
			Category: proto.String("demo-categroy"),
			Source:   proto.String("127.0.0.1"),
			LogTags: []*sls.LogTag{
				&sls.LogTag{
					Key:   proto.String("t1"),
					Value: proto.String("v1"),
				},
				&sls.LogTag{
					Key:   proto.String("t2"),
					Value: proto.String("v2"),
				},
			},
		}
		// PutLogs API Ref: https://intl.aliyun.com/help/doc-detail/29026.htm
		if err := util.Client.PutLogs(proj, store, loggroup); err != nil {
			fmt.Printf("PutLogs fail, err: %s\n", err)
		}
		fmt.Printf("#%d waiting %d seconds ...\n", loggroupIdx, 1)
		time.Sleep(time.Second) // 为什么等待 ???
	}
	return nil
}

type any interface{}

func getMap() map[string]any {
	rand.Seed(int64(uint32(time.Now().UnixNano())))
	timestamp := time.Now().Format("2006-01-02T15:04:05-07:00")
	_timestamp := time.Now().UTC().Format("2006-01-02T15:04:05Z")
	group := "demo-group"
	topic := "demo-topic"
	partition := rand.Intn(10)
	offset := rand.Intn(10000)
	return map[string]any{
		"@context": map[string]any{
			"logSize": rand.Intn(1000),
			"metadata": map[string]any{
				"group":     group,
				"timestamp": timestamp,
			},
			"offset":    offset,
			"partition": partition,
			"rawSize":   rand.Intn(1000),
			"timestamp": timestamp,
			"topic":     topic,
			"list":      []string{"log1", "log2", "log3", "log4", "log5", "log6"}, // 转换为字符串形式
		},
		"@metadata": map[string]any{
			"beat":    "filebeat",
			"topic":   topic,
			"type":    "doc",
			"version": "7.13.4",
		},
		"kubernetes": map[string]any{
			"annotations": map[string]any{
				"helm_sh/namespace": "dev",
				"helm_sh/release":   "demo",
			},
			"container_name": "c0",
			"labels": map[string]any{
				"controller_caicloud_io/chart": "app",
				"project":                      "demo",
			},
			"namespace_name": "dev",
			"pod_name":       fmt.Sprintf("pod-%08x-%04x", rand.Intn(100000), rand.Intn(100000)),
		},
		"@timestamp": _timestamp,
		"_id":        fmt.Sprintf("%s-%d-%d", topic, partition, offset),
		"cluster":    topic,
		"log":        "2022-09-08 13:04:01.760  INFO 1 --- [nio-8080-exec-3] c.h.s.service.impl.WebhooksServiceImpl   : [TID:7495231ebb3640588825ebbbf68288f0_62_16626134416111629] --- webhook 监听：  key值：key 类型： 缺陷",
		"node_name":  "kube-master-10-200-56-8",
		"offset":     offset,
		"stream":     "stdout",
	}
}

func toLog(mps map[string]any) *sls.Log {
	log := &sls.Log{
		Time: proto.Uint32(uint32(time.Now().Unix())),
	}
	for k, v := range mps {
		val, err := json.Marshal(v)
		if err != nil {
			fmt.Printf("%v\n", err)
			continue
		}
		log.Contents = append(
			log.Contents,
			&sls.LogContent{
				Key:   proto.String(k),
				Value: proto.String(string(val)),
			},
		)
	}
	return log
}

func getLogs(proj, store string, begin, end int64) error {
	// search logs from index on logstore
	total, err := getTotal(proj, store, begin, end)
	if err != nil {
		return err
	}
	offset := int64(0)
	// get logs repeatedly with (offset, lines) parameters to get complete result
	for offset < total {
		// GetLogs API Ref: https://intl.aliyun.com/help/doc-detail/29029.htm
		// 查询默认返回100
		resp, err := util.Client.GetLogs(
			proj, store, "", begin, end, "*", 150, offset, false,
		)
		if err != nil {
			fmt.Printf("GetLogs fail, err: %v\n", err)
			// time.Sleep(10 * time.Millisecond)
			// continue
			return err
		}
		fmt.Printf("Progress:%s, Count:%d, offset: %d\n", resp.Progress, resp.Count, offset)
		offset += resp.Count
		// if glResp.Count > 0 {
		// 	fmt.Printf("logs: %v\n", glResp.Logs)
		// }
		if resp.Progress == "Complete" && resp.Count == 0 {
			break
		}
	}
	return nil
}

func getTotal(proj, store string, begin, end int64) (int64, error) {
	total := int64(0)
	for {
		// GetHistograms API Ref: https://intl.aliyun.com/help/doc-detail/29030.htm
		// 获取时间区间内所有日志的条数(没有参数控制historgram的窗口大小)
		resp, err := util.Client.GetHistograms(
			proj, store, "", begin, end, "*",
		)
		if err != nil {
			fmt.Printf("GetHistograms fail, err: %v\n", err)
			// time.Sleep(10 * time.Millisecond)
			// continue
			return 0, err
		}
		fmt.Printf("complete: %s, count: %d, histograms: %v\n", resp.Progress, resp.Count, resp.Histograms)
		total += resp.Count
		if resp.Progress == "Complete" {
			break
		}
	}
	return total, nil
}
