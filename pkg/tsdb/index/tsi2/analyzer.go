package tsi2

type Analyzer struct {
	basicNum int
}

func NewAnalyzer(basicNum int) *Analyzer {
	return &Analyzer{
		basicNum: basicNum,
	}
}

func (a *Analyzer) OptimizeWithTagValuess(tagPairs []TagPair) []*TagValues {
	tagValuess := make([]*TagValues, 0, len(tagPairs))
	for i := 0; i < len(tagPairs); i++ {
		tagValuess = append(tagValuess, newTagValues(a.basicNum))
		tagValuess[i].SetValue(tagPairs[i].TagValue)
	}
	return tagValuess
}
