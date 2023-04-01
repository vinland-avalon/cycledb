package main

import (
	"context"
	"cycledb/pkg/tsdb"
	_ "cycledb/pkg/tsdb/engine"
	_ "cycledb/pkg/tsdb/index"
	"fmt"
	"log"

	"github.com/influxdata/influxdb/v2/models"
)

// var (
// 	path                string = "~/instance/engine"
// 	walPath             string = "~/instance/wal"
// 	seriesFilePath      string = "~/instance/engine/series"
// 	indexPath           string = "~/instance/index"
// 	indexSeriesFilePath string = "~/instance/index/series"
// 	database            string = "test_db"
// )

// func main() {
// 	// conf := tsdb.NewConfig()
// 	engineConf := tsdb.NewEngineOptions()
// 	index, err := tsdb.NewIndex(0, database, indexPath,
// 		tsdb.NewSeriesIDSet(), tsdb.NewSeriesFile(indexSeriesFilePath), engineConf)
// 	if err != nil {
// 		log.Fatalf("fail to init index, err: %+v", err)
// 	}
// 	err = index.Open()
// 	if err != nil {
// 		log.Fatalf("fail to open index, err: %+v", err)
// 	}

// 	engine, err := tsdb.NewEngine(0, index, path, walPath,
// 		tsdb.NewSeriesFile(indexSeriesFilePath), engineConf)
// 	if err != nil {
// 		log.Fatalf("fail to init engine, err: %+v", err)
// 	}

// 	err = engine.CreateSeriesIfNotExists([]byte("key"), []byte("name"), models.NewTags(
// 		map[string]string{
// 			"tag1": "tag_value1",
// 			"tag2": "tag_value2",
// 		},
// 	))
// 	if err != nil {
// 		log.Fatalf("fail to create series, err: %+v", err)
// 	}

// 	// shard := tsdb.NewShard(0, "", "", tsdb.NewSeriesFile(indexSeriesFilePath), engineConf)
// }

var (
	shardPath      string = "~/shard"
	shardWalPath   string = "~/shard/wal"
	seriesFilePath string = "~/shard/series"
)

func main() {
	ctx := context.Background()

	opt := tsdb.NewEngineOptions()

	seriesFile := tsdb.NewSeriesFile(seriesFilePath)

	shard := tsdb.NewShard(0, shardPath, shardWalPath, seriesFile, opt)
	err := shard.Open(ctx)
	if err != nil {
		log.Fatalf("fail to open shard, err: %+v", err)
	}
	// fmt.Printf("%+v\n", shard)

	// create a point 
	// mesurement, two fields, timestamp
	p, err := models.ParsePoints([]byte(`m v=47i,f=42 36`))
	if err != nil {
		log.Fatalf("fail to parse points, err: %+v", err)
	}

	err = shard.WritePoints(ctx, p)
	if err != nil {
		log.Fatalf("fail to write point to shard, err: %+v", err)
	}

	// cursor, err := shard.CreateCursorIterator(ctx)
	fieldSet := shard.MeasurementFields([]byte("m")).FieldSet()
	fmt.Printf("measurement %s 's fields:  %+v", "m", fieldSet)
}
