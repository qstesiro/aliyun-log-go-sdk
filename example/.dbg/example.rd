# 编译调试
{
    # project
    alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" project/log_project.go'
    alias dlv='gob && dlv exec ./log_project --init .dbg/example.dlv'

    # logstore
    alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" logstore/log_logstore.go'
    alias dlv='gob && dlv exec ./log_logstore --init .dbg/example.dlv'

    # index
    alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" index/index_sample.go'
    alias dlv='gob && dlv exec ./index_sample --init .dbg/example.dlv'

    # producer/producer_simple_demo.go
    alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" producer/producer_simple_demo.go'
    alias dlv='gob && dlv exec ./producer_simple_demo --init .dbg/example.dlv'

    # producer/simple_callback_demo.go
    alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" producer/simple_callback_demo.go'
    alias dlv='gob && dlv exec ./simple_callback_demo --init .dbg/example.dlv'
}

# 问题
{
    索引必须完整指定所有字段不能类型自动推断 ???
    是否有索引模板 ???
    store创建后无法直接写入数据 ???
    同样大小的数据读写io次数不同产生的费用不同(例如: 100条共10m数据,一次io与10次io产生的计费不同) ???
    协助评估一下费用 ???

    json类型内部不支持json嵌套 ok
      - json内数据text类型不支持"大小写敏感","分词","包含中文" ok
    是否支持直接从kafka作为数据源接入数据 ok
    索引类型只支持text,long,double,json,日志中其它类型,例如:datetime,bool如何处理 ok
    json数据类型通过cli分析(查询正常)的返回值被扁平化(体现不出json结构) ok
    当前不支持ip类型,是否有支持基于ip的地理位置转换 (logtail可以配制,但我不使用) ok
    一个project中store个数限制 ok
}
