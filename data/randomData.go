package data

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"log"
	"time"
)

type Weibo struct {
	User     string                `json:"user"`               // 用户
	Message  string                `json:"message"`            // 微博内容
	Retweets int                   `json:"retweets"`           // 转发数
	Image    string                `json:"image,omitempty"`    // 图片
	Created  time.Time             `json:"created,omitempty"`  // 创建时间
	Tags     []string              `json:"tags,omitempty"`     // 标签
	Location string                `json:"location,omitempty"` //位置
	Suggest  *elastic.SuggestField `json:"suggest_field,omitempty"`
}

const mapping = `
{
  "mappings": {
    "properties": {
      "user": {
        "type": "keyword"
      },
      "message": {
        "type": "text"
      },
      "image": {
        "type": "keyword"
      },
      "created": {
        "type": "date"
      },
      "tags": {
        "type": "keyword"
      },
      "location": {
        "type": "geo_point"
      },
      "suggest_field": {
        "type": "completion"
      }
    }
  }
}`

func randomData() {
	//client := NewClient()
	//userList := []string{
	//	"tuantuan118",
	//	"hrunze",
	//	"zmn",
	//	"hrz",
	//	"es_text",
	//	"tuan",
	//	"mengni",
	//	"elastic",
	//	"author",
	//}
	//for i := 0; i < 300; i++ {
	//	rand.Seed(time.Now().Unix() * int64(i))
	//	user := userList[rand.Intn(9)]
	//	retweets := rand.Intn(100)
	//
	//	var image string
	//	if retweets%2 == 0 {
	//		image = "~/Pictures/1.jpg"
	//	}
	//	err := createData(client, user, retweets, image)
	//	if err != nil {
	//		log.Printf("err: %s", err.Error())
	//	}
	//}
}

func createData(client *elastic.Client, user string, retweets int, image string) error {
	msg := Weibo{
		User:     user,
		Message:  "打酱油的一天",
		Retweets: retweets,
		Image:    image,
		Created:  time.Now().UTC(),
		Tags:     nil,
		Location: "",
		Suggest:  nil,
	}
	ctx := context.Background()

	put, err := client.Index().
		Index("weibo").
		BodyJson(msg).
		Do(ctx)
	if err != nil {
		return err
	}
	log.Printf("文档id: %s, 索引名: %s \n", put.Id, put.Index)
	return nil
}

func getData(client *elastic.Client) error {
	ctx := context.Background()

	get, err := client.Get().
		Index("weibo").
		Id("ZoHG838BN8vZcTp5eeZ_").
		Do(ctx)
	if err != nil {
		return err
	}
	if get.Found {
		log.Printf("文档Id: %s 版本号: %d 索引名: %s \n", get.Id, get.Version, get.Index)
	}
	msg := Weibo{}
	data, _ := get.Source.MarshalJSON()
	err = json.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	log.Println(msg)
	return nil
}

func updateData(client *elastic.Client) error {
	ctx := context.Background()

	update, err := client.Update().
		Index("weibo").
		Id("ZoHG838BN8vZcTp5eeZ_").
		Doc(map[string]interface{}{"retweets": 20}).
		Do(ctx)
	if err != nil {
		return err
	}
	log.Printf("文档id: %s, 索引名: %s \n", update.Id, update.Index)
	return nil
}

func delData(client *elastic.Client) error {
	ctx := context.Background()
	del, err := client.Delete().
		Index("weibo").
		Id("ZoHG838BN8vZcTp5eeZ_").
		Do(ctx)
	if err != nil {
		return err
	}
	log.Printf("文档id: %s, 索引名: %s \n", del.Id, del.Index)
	return nil
}

func createIndex(client *elastic.Client) error {
	ctx := context.Background()

	exists, err := client.IndexExists("weibo").Do(ctx)
	if err != nil {
		return err
	}
	if !exists {
		_, err := client.CreateIndex("weibo").BodyString(mapping).Do(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewClient() *elastic.Client {
	client, err := elastic.NewClient(
		elastic.SetURL("http://42.193.158.140:9200"),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.Printf("err: %v \n", err)
		return nil
	} else {
		log.Printf("yes \n")
	}
	return client
}
