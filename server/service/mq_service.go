package service

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// MQService RabbitMQ服务
type MQService struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queues  map[string]amqp.Queue
}

// 常量定义
const (
	FileMergeQueue = "file_merge_queue" // 文件合并队列
)

// FileTaskMessage 文件任务消息结构
type FileTaskMessage struct {
	TaskType string          `json:"taskType"` // 任务类型，如：merge, delete
	FileID   string          `json:"fileId"`   // 文件ID
	Data     json.RawMessage `json:"data"`     // 任务额外数据
}

// MergeTaksData 合并任务数据
type MergeTaskData struct {
	UserID     int    `json:"userId"`     // 用户ID
	FileHash   string `json:"fileHash"`   // 文件哈希值
	FileName   string `json:"fileName"`   // 文件名
	FileSize   int64  `json:"fileSize"`   // 文件大小
	ChunkCount int    `json:"chunkCount"` // 分片数量
	TempDir    string `json:"tempDir"`    // 临时目录
	DestDir    string `json:"destDir"`    // 目标目录
}

// NewMQService 创建新的MQ服务
func NewMQService(url string) (*MQService, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	service := &MQService{
		conn:    conn,
		channel: ch,
		queues:  make(map[string]amqp.Queue),
	}

	// 声明队列
	queues := []string{FileMergeQueue}
	for _, queueName := range queues {
		q, err := ch.QueueDeclare(
			queueName, // 队列名
			true,      // 持久化
			false,     // 自动删除
			false,     // 独占队列
			false,     // 不等待
			nil,       // 额外属性
		)
		if err != nil {
			service.Close()
			return nil, err
		}
		service.queues[queueName] = q
	}

	return service, nil
}

// Close 关闭MQ连接
func (ms *MQService) Close() error {
	if ms.channel != nil {
		ms.channel.Close()
	}
	if ms.conn != nil {
		return ms.conn.Close()
	}
	return nil
}

// PublishFileMergeTask 发布文件合并任务
func (ms *MQService) PublishFileMergeTask(ctx context.Context, fileID string, data *MergeTaskData) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	task := FileTaskMessage{
		TaskType: "merge",
		FileID:   fileID,
		Data:     dataBytes,
	}

	return ms.publishMessage(FileMergeQueue, task)
}

// 发布消息到指定队列
func (ms *MQService) publishMessage(queueName string, message interface{}) error {
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return ms.channel.Publish(
		"",        // 交换机
		queueName, // 路由键
		false,     // 强制发布
		false,     // 立即发布
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // 持久化消息
			ContentType:  "application/json",
			Body:         body,
			Timestamp:    time.Now(),
		},
	)
}

// ConsumeFileMergeTasks 消费文件合并任务
func (ms *MQService) ConsumeFileMergeTasks(handler func(FileTaskMessage) error) error {
	msgs, err := ms.channel.Consume(
		FileMergeQueue, // 队列名
		"",             // 消费者标签
		false,          // 自动确认
		false,          // 独占消费者
		false,          // 不等待
		false,          // 没有局部qos
		nil,            // 额外参数
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			var task FileTaskMessage
			err := json.Unmarshal(msg.Body, &task)
			if err != nil {
				log.Printf("解析消息失败: %v", err)
				msg.Nack(false, false) // 拒绝消息，不重新入队
				continue
			}

			err = handler(task)
			if err != nil {
				log.Printf("处理任务失败: %v", err)
				// 如果任务处理失败，将消息重新放入队列
				msg.Nack(false, true)
			} else {
				// 任务处理成功，确认消息
				msg.Ack(false)
			}
		}
	}()

	return nil
}
