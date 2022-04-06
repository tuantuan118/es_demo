package main

import (
	"context"
	"github.com/olivere/elastic/v7"
	"log"
	"os"
	"time"
)

func main() {
	client := newClient()
	if client == nil {
		return
	}

	ctx := context.Background()

	termQuery := elastic.NewTermQuery("user", "tuantuan118")
	aggs := elastic.NewTermsAggregation().Field("user")
	searchResult, err := client.Search().
		Index("weibo").
		Query(termQuery).
		//Query(elastic.NewMatchAllQuery()).
		Aggregation("user", aggs).
		Sort("created", true).
		Pretty(true).
		Do(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("查询消耗时间 %d ms, 结果总数: %d\n", searchResult.TookInMillis, searchResult.TotalHits())

	agg, found := searchResult.Aggregations.Terms("user")
	if !found {
		log.Fatal("没有找到聚合数据")
	}

	for _, bucket := range agg.Buckets {
		bucketValue := bucket.Key
		log.Printf("bucket = %q 文档总数 = %d\n", bucketValue, bucket.DocCount)
	}
}

func newClient() *elastic.Client {
	client, err := elastic.NewClient(
		elastic.SetURL("http://42.193.158.140:9200"),
		elastic.SetGzip(true),
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC", log.LstdFlags)),
		elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.Printf("连接失败: %v \n", err)
		return nil
	}
	return client
}
