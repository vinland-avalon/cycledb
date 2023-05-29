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

func (g *RandomGenerator) GenerateInsertTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	dataFile := g.getFilePath(tagKeyNum, tagValueNum)
	// check or create data file
	if _, err := os.Stat(dataFile); err != nil {
		if os.IsNotExist(err) {
			seriesKeys := g.generateRandomTagPairSets(tagKeyNum, tagValueNum)
			g.createAndWriteToFile(seriesKeys, dataFile)
		} else {
			fmt.Printf("[GenerateInsertTagPairs] fail to open file: %v, err: %+v\n", dataFile, err)
			os.Exit(0)
		}
	}

	// read data file and construct series keys (tag pairs)
	return g.readTagPairSetsFromFile(dataFile)
}

func (g *RandomGenerator) GenerateQueryTagPairSets(tagKeyNum, tagValueNum int) [][]tsi2.TagPair {
	// generate original tag pairs
	FPGen := FullPermutationGen{}
	return FPGen.GenerateQueryTagPairSets(tagKeyNum, tagValueNum)
}

func (g *RandomGenerator) generateRandomTagPairSets(tagKeyNum, tagValueNum int) []string {
	// generate original tag pairs
	FPGen := FullPermutationGen{}
	orginalTagPairSets := FPGen.GenerateQueryTagPairSets(tagKeyNum, tagValueNum)

	// delete partially and repeat partially from the orginal tag pairs, 3 * 50% = 1.5
	tagPairSets := [][]tsi2.TagPair{}
	for i := 0; i < 3; i++ {
		for _, tagPairSet := range orginalTagPairSets {
			// choose 50% tag pairs randomly
			rand.Seed(time.Now().UnixNano())
			choose := rand.Intn(2)
			if choose == 1 {
				tagPairSets = append(tagPairSets, tagPairSet)
			}
		}
	}
	// shuffle
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(tagPairSets), func(i, j int) {
		tagPairSets[i], tagPairSets[j] = tagPairSets[j], tagPairSets[i]
	})

	// serialize to []string
	tagPairSetsStrings := []string{}
	for _, tagPairSet := range tagPairSets {
		tagPairSetString := ""
		for _, tagPair := range tagPairSet {
			tagPairSetString += fmt.Sprintf("%s\t%s\t", tagPair.TagKey, tagPair.TagValue)
		}
		tagPairSetsStrings = append(tagPairSetsStrings, tagPairSetString)
	}
	return tagPairSetsStrings
}

func (g *RandomGenerator) getFilePath(tagKeyNum, tagValueNumm int) string {
	return fmt.Sprintf("%s_%d_%d", RANDOM_GENERATOR_FILE_PATH, tagKeyNum, tagValueNumm)
}

func (g *RandomGenerator) createAndWriteToFile(tagPairSets []string, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, line := range tagPairSets {
		_, err := f.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *RandomGenerator) readTagPairSetsFromFile(filePath string) [][]tsi2.TagPair {
	f, _ := os.Open(filePath)
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)

	tagPairSets := [][]tsi2.TagPair{}

	for fileScanner.Scan() {
		tagPairSets = append(tagPairSets, convertToTagPairs(fileScanner.Text()))
	}
	return tagPairSets
}

func convertToTagPairs(line string) []tsi2.TagPair {
	// delete `\t` and `\n` at back
	// TODO(vinland-avalon): error handle
	line = line[:len(line)-1]
	elems := strings.Split(line, "\t")
	tagPairSet := []tsi2.TagPair{}
	for i := 0; i < len(elems); i += 2 {
		tagPairSet = append(tagPairSet, tsi2.TagPair{TagKey: elems[i], TagValue: elems[i+1]})
	}
	return tagPairSet
}
