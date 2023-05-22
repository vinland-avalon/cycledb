SHELL=/bin/bash

TEAM:=ASU
NAME:=cycledb

# cd ./pkg/tsdb/index/tsi2

fmt:
	go fmt ./...

test:
	go test -v ./...

benchmark:
	go test -bench=. -run=none -benchmem

benchmark_params:
	go test -bench=. -run=none -benchmem -args -tagKeyNum=$(TAG_KEY_NUM) -tagValueNum=$(TAG_VALUE_NUM)

benchmark_pprof:
	go test -bench=. -run=none -benchmem -memprofile=./benchmark_result/mem_profile.out -cpuprofile=./benchmark_result/cpu_profile.out


