package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"cycledb/pkg/tsdb"
	_ "cycledb/pkg/tsdb/engine"
	_ "cycledb/pkg/tsdb/index"
	"cycledb/pkg/tsdb/index/tsi1"
	"io"
	"log"
	"os"
	"time"

	"github.com/influxdata/influxdb/v2/models"
)

var (
	shardPath      string = "shard"
	shardWalPath   string = "wal"
	seriesFilePath string = "series_file"
)

func main() {
	basePath := "../instance/" + time.Now().Format(time.RFC850) + "/"
	shardPath = basePath + shardPath
	shardWalPath = basePath + shardWalPath
	seriesFilePath = basePath + seriesFilePath

	// 0. init shard
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

	// 1. create a point
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

	// 2. create batch of points
	points, err := batchOfTestData("./pkg/tsdb/index/tsi1/testdata/line-protocol-1M.txt.gz")
	if err != nil {
		log.Fatalf("fail to reach zipped test data, err: %+v", err)
	}
	err = shard.WritePoints(ctx, points)
	if err != nil {
		log.Fatalf("fail to write batch of data, err: %+v", err)
	}

	itr, err := shard.CreateCursorIterator(ctx)
	if err != nil {
		log.Fatalf("fail to create iterator of shard, err: %+v\n", err)
	}

	log.Printf("itr.status: %+v\n", itr.Stats())

	time.Sleep(5 * time.Second)
	shard.Close()
}

func batchOfTestData(zipPath string) ([]models.Point, error) {
	fd, err := os.Open(zipPath)
	if err != nil {
		return nil, err
	}

	gzr, err := gzip.NewReader(fd)
	if err != nil {
		fd.Close()
		return nil, err
	}

	data, err := io.ReadAll(gzr)
	if err != nil {
		return nil, err
	}

	if err := fd.Close(); err != nil {
		return nil, err
	}

	return models.ParsePoints(data)
}

func exportIndex(idx *tsi1.Index) (bytes.Buffer, error) {
	var buf bytes.Buffer
	e := tsi1.NewSQLIndexExporter(&buf)
	e.ShowSchema = false
	err := e.ExportIndex(idx)
	return buf, err
}
