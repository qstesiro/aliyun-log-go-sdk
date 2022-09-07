package main

import (
	"fmt"
	"math/rand"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
	"github.com/gogo/protobuf/proto"
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
	fmt.Printf("waiting %d seconds ...\n", WAITING*2)
	time.Sleep(WAITING * 2 * time.Second) // 为什么等待 ???
	fmt.Printf("PutLogs project: %s, store: %s\n", util.ProjectName, logstore_name)
	if err := PutLogs(util.ProjectName, logstore_name); err != nil {
		fmt.Printf("PutLogs fail, err: %s\n", err)
		return
	}
	// end_time := uint32(time.Now().Unix())
	fmt.Printf("waiting %d seconds ...\n", WAITING)
	time.Sleep(WAITING * time.Second) // 为什么等待 ???
	// search logs from index on logstore
	// totalCount := int64(0)
	// for {
	// 	// GetHistograms API Ref: https://intl.aliyun.com/help/doc-detail/29030.htm
	// 	ghResp, err := util.Client.GetHistograms(util.ProjectName, logstore_name, "", int64(begin_time), int64(end_time), "col_0 > 1000000")
	// 	if err != nil {
	// 		fmt.Printf("GetHistograms fail, err: %v\n", err)
	// 		time.Sleep(10 * time.Millisecond)
	// 		continue
	// 	}
	// 	fmt.Printf("complete: %s, count: %d, histograms: %v\n", ghResp.Progress, ghResp.Count, ghResp.Histograms)
	// 	totalCount += ghResp.Count
	// 	if ghResp.Progress == "Complete" {
	// 		break
	// 	}
	// }
	// offset := int64(0)
	// // get logs repeatedly with (offset, lines) parameters to get complete result
	// for offset < totalCount {
	// 	// GetLogs API Ref: https://intl.aliyun.com/help/doc-detail/29029.htm
	// 	glResp, err := util.Client.GetLogs(util.ProjectName, logstore_name, "", int64(begin_time), int64(end_time), "col_0 > 1000000", 100, offset, false)
	// 	if err != nil {
	// 		fmt.Printf("GetLogs fail, err: %v\n", err)
	// 		time.Sleep(10 * time.Millisecond)
	// 		continue
	// 	}
	// 	fmt.Printf("Progress:%s, Count:%d, offset: %d\n", glResp.Progress, glResp.Count, offset)
	// 	offset += glResp.Count
	// 	if glResp.Count > 0 {
	// 		fmt.Printf("logs: %v\n", glResp.Logs)
	// 	}
	// 	if glResp.Progress == "Complete" && glResp.Count == 0 {
	// 		break
	// 	}
	// }
	fmt.Println("index sample end")
}

func CreateStore(proj, store string) error {
	util.Client.DeleteLogStore(proj, store)
	fmt.Printf("waiting %d seconds ...\n", WAITING)
	time.Sleep(WAITING * time.Second) // 为什么等待 ???
	err := util.Client.CreateLogStore(proj, store, 1, 2, true, 16)
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

func CreateIndex(proj, store string) error {
	return util.Client.CreateIndex(
		proj, store,
		sls.Index{
			// 指定字段索引信息
			Keys: map[string]sls.IndexKey{
				// JsonKey
				"col_0": {
					Type:          "json",
					Alias:         "col0",
					CaseSensitive: false,
					Token:         []string{",", ":", " ", "-"}, // 分词符
					Chn:           true,
					DocValue:      true,
					JsonKeys: map[string]*sls.JsonKey{
						"k0": &sls.JsonKey{
							Type:     "text",
							DocValue: true,
						},
						"k1": &sls.JsonKey{
							Type:     "long",
							DocValue: true,
						},
						"k2.k0.k0": &sls.JsonKey{ // 第二级及后续级别必须扁平化
							Type:     "text",
							DocValue: true,
						},
						"k2.k0.k1": &sls.JsonKey{ // 第二级及后续级别必须扁平化
							Type:     "long",
							DocValue: true,
						},
						"k2.k1": &sls.JsonKey{ // 第二级及后续级别必须扁平化
							Type:     "long",
							DocValue: true,
						},
					},
				},
				// 展开Json
				// "col_0.key_0": {
				// 	Type:          "text",
				// 	Alias:         "col0.key0",
				// 	CaseSensitive: false,
				// 	Token:         []string{",", ":", " "}, // 分词符
				// 	Chn:           true,
				// 	DocValue:      true,
				// },
				// "col_0.key_1": {
				// 	Type:     "long",
				// 	DocValue: true,
				// },
				"col_1": {
					Type:     "long",
					DocValue: false,
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

func PutLogs(proj, store string) error {
	begin_time := uint32(time.Now().Unix())
	rand.Seed(int64(begin_time))
	// put logs to logstore
	// 写入10组每组100条每条10个字段,只有col_0/1有索引项
	// 数据写入不稳定会少数据(因为是通过公网) ???
	groupMax, logMax := 1, 10
	for loggroupIdx := 0; loggroupIdx < groupMax; loggroupIdx++ {
		logs := []*sls.Log{}
		for logIdx := 0; logIdx < logMax; logIdx++ {
			content := []*sls.LogContent{}
			for colIdx := 0; colIdx < 10; colIdx++ {
				if colIdx == 0 {
					// JsonKey
					content = append(
						content,
						&sls.LogContent{
							Key: proto.String("col_0"),
							Value: proto.String(
								fmt.Sprintf(
									`{
                                       "k0": "text-%d",
                                       "k1": %d,
                                       "k2": {
                                         "k0": %d,
                                         "k1": {
                                           "k0": "text-%d",
                                           "k1": %d
                                         }
                                       }
                                     }`,
									rand.Intn(1000),
									rand.Intn(1000),
									rand.Intn(1000),
									rand.Intn(1000),
									rand.Intn(1000),
								),
							),
						},
					)
					// 展开Json
					// content = append(
					// 	content,
					// 	[]*sls.LogContent{
					// 		&sls.LogContent{
					// 			Key:   proto.String(fmt.Sprintf("col_%d.key_0", colIdx)),
					// 			Value: proto.String(fmt.Sprintf("text-%d", rand.Intn(10000000))),
					// 		},
					// 		&sls.LogContent{
					// 			Key:   proto.String(fmt.Sprintf("col_%d.key_1", colIdx)),
					// 			Value: proto.String(fmt.Sprintf("%d", rand.Intn(10000000))),
					// 		},
					// 	}...,
					// )
				} else {
					content = append(content, &sls.LogContent{
						Key:   proto.String(fmt.Sprintf("col_%d", colIdx)),
						Value: proto.String(fmt.Sprintf("loggroup idx: %d, log idx: %d, col idx: %d, value: %d", loggroupIdx, logIdx, colIdx, rand.Intn(10000000))),
					})
				}
			}
			log := &sls.Log{
				Time:     proto.Uint32(uint32(time.Now().Unix())),
				Contents: content,
			}
			logs = append(logs, log)
		}
		loggroup := &sls.LogGroup{
			Logs:     logs,
			Topic:    proto.String("demo-topic"),
			Category: proto.String("demo-categroy"),
			Source:   proto.String("10.230.201.117"),
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
