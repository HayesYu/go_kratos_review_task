package task

import (
	"context"
	"encoding/json"
	"review-task/internal/conf"

	"github.com/go-kratos/kratos/v2/errors"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/segmentio/kafka-go"
)

// 评价数据流处理任务

// TaskWorker 自定义执行task的结构体，实现transport.Server
type TaskWorker struct {
	kafkaReader *kafka.Reader // kafka reader
	esClient    *ESClient     // ES client
	log         *log.Helper
}

type ESClient struct {
	Client *elasticsearch.TypedClient
	Index  string
}

func NewTaskWorker(kafkaReader *kafka.Reader, esClient *ESClient, logger log.Logger) *TaskWorker {
	return &TaskWorker{
		kafkaReader: kafkaReader,
		esClient:    esClient,
		log:         log.NewHelper(logger),
	}
}

func NewKafkaReader(cfg *conf.Kafka) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		GroupID: cfg.GroupId, // 指定消费者组id
		Topic:   cfg.Topic,
		//MaxBytes: 10e6, // 10MB
	})
}

func NewESClient(cfg *conf.Elasticsearch) (*ESClient, error) {
	// ES 配置
	newCfg := elasticsearch.Config{
		Addresses: cfg.Addresses,
	}

	// 创建客户端连接
	client, err := elasticsearch.NewTypedClient(newCfg)
	if err != nil {
		return nil, err
	}
	return &ESClient{
		Client: client,
		Index:  cfg.Index,
	}, nil
}

// Msg 定义kafka中接收到的数据
type Msg struct {
	Type     string                   `json:"type"`
	DataBase string                   `json:"database"`
	Table    string                   `json:"table"`
	IsDdl    bool                     `json:"isDdl"`
	Data     []map[string]interface{} `json:"data"`
}

// Start 程序启动之后会调用的方法
// ctx 是kratos框架启动的时候传入的，带有退出取消
func (ts TaskWorker) Start(ctx context.Context) error {
	ts.log.Debug("task worker starting...")
	// 1.从kafka中获取mysql中的数据变更消息
	// 接收消息
	for {
		m, err := ts.kafkaReader.ReadMessage(ctx)
		if errors.Is(err, context.Canceled) {
			return nil // 如果是context取消错误，直接返回
		}
		if err != nil {
			ts.log.Errorf("failed to read message from kafka, err: %v", err)
			break
		}
		ts.log.Debugf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		// 2.将完整评价数据写入ES
		msg := new(Msg)
		err = json.Unmarshal(m.Value, msg)
		if err != nil {
			ts.log.Errorf("failed to unmarshal message from kafka, err: %v", err)
			continue
		}
		// 实际的业务场景可能需要在这增加一步数据处理
		// 例如把两张表的数据合成一个文档写入ES
		if msg.Type == "INSERT" {
			// 往ES新增文档
			for idx := range msg.Data {
				ts.indexDocument(msg.Data[idx])
			}
		} else {
			// 往ES更新文档
			for idx := range msg.Data {
				ts.updateDocument(msg.Data[idx])
			}
		}
	}
	return nil
}

// Stop 程序结束之后会调用的方法
func (ts TaskWorker) Stop(context.Context) error {
	ts.log.Debug("task worker stopping...")
	// 关闭kafka reader
	return ts.kafkaReader.Close()
}

// indexDocument 索引文档
func (ts TaskWorker) indexDocument(d map[string]interface{}) {
	reviewID := d["review_id"].(string)
	// 添加文档
	resp, err := ts.esClient.Client.Index(ts.esClient.Index).
		Id(reviewID).
		Document(d).
		Do(context.Background())
	if err != nil {
		ts.log.Errorf("indexing document failed, err:%v\n", err)
		return
	}
	ts.log.Debugf("result:%#v\n", resp.Result)
}

// updateDocument 更新文档
func (ts TaskWorker) updateDocument(d map[string]interface{}) {
	reviewID := d["review_id"].(string)
	resp, err := ts.esClient.Client.Update(ts.esClient.Index, reviewID).
		Doc(d). // 使用结构体变量更新
		Do(context.Background())
	if err != nil {
		ts.log.Errorf("update document failed, err:%v\n", err)
		return
	}
	ts.log.Debugf("result:%v\n", resp.Result)
}
