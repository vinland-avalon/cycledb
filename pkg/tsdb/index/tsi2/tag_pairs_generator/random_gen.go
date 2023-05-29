package generator

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"cycledb/pkg/tsdb/index/tsi2"
)

const RANDOM_GENERATOR_FILE_PATH = "./data/series_keys"

type RandomGenerator struct{}

func (g *RandomGenerator) GenerateInsertTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	dataFile := g.getFilePath(tagKeyNum, tagValueNum)
	// check or create data file
	if _, err := os.Stat(dataFile); err != nil {
		if os.IsNotExist(err) {
			seriesKeys := g.generateRandomSeriesKeys(tagKeyNum, tagValueNum)
			g.createAndWriteToFile(seriesKeys, dataFile)
		} else {
			fmt.Printf("[GenerateInsertTagPairs] fail to open file: %v, err: %+v\n", dataFile, err)
			os.Exit(0)
		}
	}

	// read data file and construct series keys (tag pairs)
	return g.readSeriesKeysFromFile(dataFile)
}

func (g *RandomGenerator) GenerateQueryTagPairs(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	// generate original tag pairs
	FPGen := FullPermutationGen{}
	return FPGen.GenerateQueryTagPairs(tagKeyNum, tagValueNum)
}

func (g *RandomGenerator) generateRandomSeriesKeys(tagKeyNum, tagValueNum int) []string {
	// generate original tag pairs
	FPGen := FullPermutationGen{}
	orginalTagPairs := FPGen.GenerateQueryTagPairs(tagKeyNum, tagValueNum)

	// delete partially and repeat partially from the orginal tag pairs, 3 * 50% = 1.5
	tagPairs := [][]tsi2.TagPair{}
	for i := 0; i < 3; i++ {
		for _, tagPair := range orginalTagPairs {
			// choose 50% tag pairs randomly
			rand.Seed(time.Now().UnixNano())
			choose := rand.Intn(2)
			if choose == 1 {
				tagPairs = append(tagPairs, tagPair)
			}
		}
	}
	// shuffle
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(tagPairs), func(i, j int) {
		tagPairs[i], tagPairs[j] = tagPairs[j], tagPairs[i]
	})

	// serialize to []string
	tagPairsStrings := []string{}
	for _, tagPair := range tagPairs {
		tagPairString := ""
		for _, singleTagPair := range tagPair {
			tagPairString += fmt.Sprintf("%s\t%s\t", singleTagPair.TagKey, singleTagPair.TagValue)
		}
		tagPairsStrings = append(tagPairsStrings, tagPairString)
	}
	return tagPairsStrings
}

func (g *RandomGenerator) getFilePath(tagKeyNum, tagValueNumm int) string {
	return fmt.Sprintf("%s_%d_%d", RANDOM_GENERATOR_FILE_PATH, tagKeyNum, tagValueNumm)
}

func (g *RandomGenerator) createAndWriteToFile(seriesKeys []string, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, line := range seriesKeys {
		_, err := f.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *RandomGenerator) readSeriesKeysFromFile(filePath string) [][]tsi2.TagPair {
	f, _ := os.Open(filePath)
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)

	seriesKeys := [][]tsi2.TagPair{}

	for fileScanner.Scan() {
		seriesKeys = append(seriesKeys, convertToTagPairs(fileScanner.Text()))
	}
	return seriesKeys
}

func convertToTagPairs(line string) []tsi2.TagPair {
	// delete `\t` and `\n` at back
	// TODO(vinland-avalon): error handle
	line = line[:len(line)-1]
	elems := strings.Split(line, "\t")
	tagPairs := []tsi2.TagPair{}
	for i := 0; i < len(elems); i += 2 {
		tagPairs = append(tagPairs, tsi2.TagPair{TagKey: elems[i], TagValue: elems[i+1]})
	}
	return tagPairs
}
