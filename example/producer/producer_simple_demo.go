package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
	"github.com/aliyun/aliyun-log-go-sdk/producer"
)

func main() {
	logstore_name := "test"
	fmt.Printf("createStore project: %s, store: %s\n", util.ProjectName, logstore_name)
	if err := createStore(util.ProjectName, logstore_name); err != nil {
		fmt.Printf("createStore fail, err: %s\n", err)
		return
	}
	fmt.Printf("waiting %d seconds ...\n", WAITING)
	time.Sleep(WAITING * time.Second) // 为什么等待 ???
	fmt.Printf("createIndex project: %s, store: %s\n", util.ProjectName, logstore_name)
	if err := createIndex(util.ProjectName, logstore_name); err != nil {
		fmt.Printf("createIndex fail, err: %s\n", err)
		return
	}
	fmt.Printf("waiting %d seconds ...\n", WAITING*3)
	time.Sleep(WAITING * 3 * time.Second) // 为什么等待 ???
	sign := make(chan os.Signal)
	signal.Notify(sign, os.Kill, os.Interrupt)
	producer := getProducer()
	defer producer.SafeClose()
	// sendLog(producer, util.ProjectName, logstore_name) // 同步
	asyncSendLog(producer, util.ProjectName, logstore_name) // 异步
	if _, ok := <-sign; ok {
		fmt.Println("Get the shutdown signal and start to shut down")
	}
}

const (
	WAITING = 15
)

func createStore(proj, store string) error {
	util.Client.DeleteLogStore(proj, store)
	return util.Client.CreateLogStore(proj, store, 90, 2, true, 16)
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
						"metadata.timestamp": &sls.JsonKey{ // map必须扁平化
							Type:     "text",
							DocValue: true,
						},
						"offset": &sls.JsonKey{ // map必须扁平化
							Type:     "long",
							DocValue: true,
						},
						"partition": &sls.JsonKey{ // map必须扁平化
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
				Value: proto.String(*(*string)(unsafe.Pointer(&val))), // 优化
			},
		)
	}
	return log
}

func getProducer() *producer.Producer {
	config := producer.GetDefaultProducerConfig()
	config.AccessKeyID = os.Getenv("ACCESS_KEY_ID")
	config.AccessKeySecret = os.Getenv("ACCESS_KEY_SECRET")
	config.Endpoint = os.Getenv("ENDPOINT")
	config.MaxReservedAttempts = 3
	return producer.InitProducer(config)
}

const (
	GORMAX = 10
	BATCH  = 100
	COUNT  = 1000
)

func sendLog(producer *producer.Producer, proj, store string) {
	producer.Start()
	var m sync.WaitGroup
	for i := 0; i < GORMAX; i += 1 {
		m.Add(1)
		go func(idx int) {
			defer m.Done()
			for i := 0; i < BATCH; i += 1 {
				lst := []*sls.Log{}
				for i := 0; i < COUNT; i += 1 {
					// GenerateLog  is producer's function for generating SLS format logs
					// GenerateLog has low performance, and native Log interface is the best choice for high performance.
					// log := producer.GenerateLog(uint32(time.Now().Unix()), map[string]string{"content": "test", "content2": fmt.Sprintf("%v", i)})
					lst = append(lst, toLog(getMap()))
				}
				if err := producer.SendLogList(
					proj, store, "demo-topic", "127.0.0.1", lst,
				); err != nil {
					fmt.Println(err)
				}
			}
			fmt.Printf("#%d send completion\n", idx)
		}(i)
	}
	m.Wait()
	fmt.Printf("all send completion\n")
}

func asyncSendLog(producer *producer.Producer, proj, store string) {
	producer.Start()
	var m sync.WaitGroup
	callback := new(Callback)
	for i := 0; i < GORMAX; i += 1 {
		m.Add(1)
		go func(idx int) {
			defer m.Done()
			for i := 0; i < BATCH; i += 1 {
				lst := []*sls.Log{}
				for i := 0; i < COUNT; i += 1 {
					// GenerateLog  is producer's function for generating SLS format logs
					// GenerateLog has low performance, and native Log interface is the best choice for high performance.
					// log := producer.GenerateLog(uint32(time.Now().Unix()), map[string]string{"content": "test", "content2": fmt.Sprintf("%v", i)})
					lst = append(lst, toLog(getMap()))
				}
				if err := producer.SendLogListWithCallBack(
					proj, store, "demo-topic", "127.0.0.1", lst, callback,
				); err != nil {
					fmt.Println(err)
				}
			}
			fmt.Printf("#%d send completion\n", idx)
		}(i)
	}
	m.Wait()
	fmt.Printf("all send completion\n")
}

type Callback struct {
}

func (r *Callback) Success(res *producer.Result) {
	// attemptList := res.GetReservedAttempts() // 遍历获得所有的发送记录
	// for _, attempt := range attemptList {
	// 	fmt.Println(attempt)
	// }
}

func (r *Callback) Fail(res *producer.Result) {
	fmt.Printf(
		"id: %v, code: %v, message: %v\n",
		res.GetRequestId(),
		res.GetErrorCode(),
		res.GetErrorMessage(),
	)
	// fmt.Println(res.IsSuccessful())        // 获得发送日志是否成功
	// fmt.Println(res.GetErrorCode())        // 获得最后一次发送失败错误码
	// fmt.Println(res.GetErrorMessage())     // 获得最后一次发送失败信息
	// fmt.Println(res.GetReservedAttempts()) // 获得producerBatch 每次尝试被发送的信息
	// fmt.Println(res.GetRequestId())        // 获得最后一次发送失败请求Id
	// fmt.Println(res.GetTimeStampMs())      // 获得最后一次发送失败请求时间
}
