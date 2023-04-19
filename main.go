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
	// shard.WithLogger(&zap.Logger{})
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
	log.Printf("measurement %s 's fields:  %+v", "m", fieldSet)

	itr, err := shard.CreateCursorIterator(ctx)
	if err != nil {
		log.Fatalf("fail to create iterator of shard, err: %+v\n", err)
	}

	fmt.Printf("itr.status: %+v\n", itr.Stats())
}
