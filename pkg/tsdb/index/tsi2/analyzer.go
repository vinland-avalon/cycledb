package tsi2

var (
	basicNum = 10
)

type Analyzer struct {
}

func (a *Analyzer) OptimizeWithTagValuess(tagPairs []TagPair) []*TagValues {
	tagValuess := make([]*TagValues, 0, len(tagPairs))
	for i := 0; i < len(tagPairs); i++ {
		tagValuess = append(tagValuess, newTagValues(basicNum))
		tagValuess[i].SetValue(tagPairs[i].TagValue)
	}
	return tagValuess
}
