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

# make benchmark_params TAG_KEY_NUM=3 TAG_VALUE_NUM=10 SERIES_KEY_GENERATOR=full_permutation_generator
benchmark_params:
	go test -bench=. -run=none -benchmem -args -tagKeyNum=$(TAG_KEY_NUM) -tagValueNum=$(TAG_VALUE_NUM) -seriesKeyGenerator=$(SERIES_KEY_GENERATOR)

# benchmark_pprof_query:
# 	go test -run=none -bench BenchmarkGridIndexQuery -benchmem -memprofile=./benchmark_result/mem_profile.out -cpuprofile=./benchmark_result/cpu_profile.out

benchmark_pprof:
	go test -bench=. -run=none -benchmem -memprofile=./benchmark_result/mem_profile.out -cpuprofile=./benchmark_result/cpu_profile.out -args -tagKeyNum=$(TAG_KEY_NUM) -tagValueNum=$(TAG_VALUE_NUM)


