package tsi2

import (
	"cycledb/pkg/tsdb"
	"sync"

	"github.com/influxdata/influxdb/v2/models"
)

// for benchmark comparation
type InvertIndex struct {
	// tagKey -> tagValue -> series ids
	invertIndex map[string]map[string]map[uint64]struct{}
	// id -> tag pairs
	idToTags map[uint64]models.Tags
	// as increasing id, begin with 1, which means id begins at 1
	idCnt uint64
	// TODO(vinland-avalon): for high performance, improvements include:
	// 1. partition
	// 2. limit the zone that lock protects
	mu sync.RWMutex

	seriesIDSet *tsdb.SeriesIDSet
}

func NewInvertIndex() *InvertIndex {
	return &InvertIndex{
		invertIndex: map[string]map[string]map[uint64]struct{}{},
		idToTags:    map[uint64]models.Tags{},
		idCnt:       1,
		mu:          sync.RWMutex{},
		seriesIDSet: tsdb.NewSeriesIDSet(),
	}
}

func (ii *InvertIndex) GetSeriesIDsWithTagPairSet(tags models.Tags) []uint64 {
	ii.mu.RLock()
	defer ii.mu.RUnlock()

	return ii.getSeriesIDsWithTagPairSet(tags)
}

func (ii *InvertIndex) getSeriesIDsWithTagPairSet(tags models.Tags) []uint64 {
	copyMapI64 := func(ids map[uint64]struct{}) map[uint64]struct{} {
		m := map[uint64]struct{}{}
		for id := range ids {
			m[id] = struct{}{}
		}
		return m
	}

	ids := []uint64{}

	if len(tags) == 0 {
		ii.seriesIDSet.ForEach(func(id uint64) {
			ids = append(ids, id)
		})
	}

	idSet := copyMapI64(ii.getSeriesIDsForSingleTagPair(tags[0]))
	for i := 1; i < len(tags); i++ {
		currIdsSet := ii.getSeriesIDsForSingleTagPair(tags[i])
		for k := range idSet {
			if _, ok := currIdsSet[k]; !ok {
				delete(idSet, k)
			}
		}
	}

	for k := range idSet {
		ids = append(ids, k)
	}
	return ids
}

func (ii *InvertIndex) getSeriesIDsForSingleTagPair(tag models.Tag) map[uint64]struct{} {
	return ii.invertIndex[string(tag.Key)][string(tag.Value)]
}

func (ii *InvertIndex) SetTagPairSet(tags models.Tags) (bool, uint64) {
	// check if the tagPairs already exists in index
	ii.mu.RLock()
	if idFound := ii.getStrictlyMatchedSeriesIDForTagPairs(tags); idFound != 0 {
		defer ii.mu.RUnlock()
		return false, idFound
	}

	ii.mu.RUnlock()
	ii.mu.Lock()
	defer ii.mu.Unlock()
	// double check
	if idFound := ii.getStrictlyMatchedSeriesIDForTagPairs(tags); idFound != 0 {
		return false, idFound
	}
	// do the insert
	currId := ii.idCnt
	for _, tag := range tags {
		if _, ok := ii.invertIndex[string(tag.Key)]; !ok {
			ii.invertIndex[string(tag.Key)] = map[string]map[uint64]struct{}{}
		}
		if _, ok := ii.invertIndex[string(tag.Key)][string(tag.Value)]; !ok {
			ii.invertIndex[string(tag.Key)][string(tag.Value)] = map[uint64]struct{}{}
		}
		ii.invertIndex[string(tag.Key)][string(tag.Value)][currId] = struct{}{}
	}

	ii.idToTags[currId] = tags
	ii.idCnt++
	ii.seriesIDSet.Add(uint64(currId))
	return true, currId
}

func (ii *InvertIndex) getStrictlyMatchedSeriesIDForTagPairs(tags models.Tags) uint64 {
	ids := ii.getSeriesIDsWithTagPairSet(tags)
	for _, id := range ids {
		if ii.idToTags[id].Equal(tags) {
			return id
		}
	}
	return 0
}
