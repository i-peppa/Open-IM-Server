package redismq

import (
	"Open_IM/pkg/common/config"
	"Open_IM/pkg/common/log"
	pbChat "Open_IM/pkg/proto/msg"
	"Open_IM/pkg/utils"
	"context"
	go_redis "github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
)

type Producer struct {
	RDB go_redis.UniversalClient
}

func NewProducer() *Producer {
	p := Producer{}
	p.RDB = go_redis.NewClusterClient(&go_redis.ClusterOptions{
		Addrs:    config.Config.Redis.DBAddress,
		Username: config.Config.Redis.DBUserName,
		Password: config.Config.Redis.DBPassWord, // no password set
		PoolSize: 50,
	})
	return &p
}

func (p *Producer) SendMessage(m proto.Message, key string, operationID string) (int32, int64, error) {
	bMsg, _ := proto.Marshal(m)
	p.RDB.RPush(context.Background(), "msg", bMsg)
	pop := p.RDB.RPop(context.Background(), "msg")
	bytes, _ := pop.Bytes()
	msgFromMQ := pbChat.PushMsgDataToMQ{}
	if err := proto.Unmarshal(bytes, &msgFromMQ); err != nil {
		log.Error("", "push Unmarshal msg err", "msg", string(msg), "err", err.Error())
	}
	pb2String, _ := utils.Pb2String(m)
	log.Info("senddebug ", pb2String)
	return 0, 0, nil
}
