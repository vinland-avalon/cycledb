package generator

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2/models"
)

const RANDOM_GENERATOR_FILE_PATH = "./data/series_keys"

type RandomGenerator struct{}

func (g *RandomGenerator) GenerateInsertTagsSlice(tagKeyNum, tagValueNum int) []models.Tags {
	dataFile := g.getFilePath(tagKeyNum, tagValueNum)
	// check or create data file
	if _, err := os.Stat(dataFile); err != nil {
		if os.IsNotExist(err) {
			seriesKeys := g.generateRandomtagsSlice(tagKeyNum, tagValueNum)
			g.createAndWriteToFile(seriesKeys, dataFile)
		} else {
			fmt.Printf("[GenerateInsertTagPairs] fail to open file: %v, err: %+v\n", dataFile, err)
			os.Exit(0)
		}
	}

	// read data file and construct series keys (tag pairs)
	return g.readtagsSliceFromFile(dataFile)
}

func (g *RandomGenerator) GenerateQueryTagsSlice(tagKeyNum, tagValueNum int) []models.Tags {
	// generate original tag pairs
	FPGen := FullPermutationGen{}
	return FPGen.GenerateQueryTagsSlice(tagKeyNum, tagValueNum)
}

func (g *RandomGenerator) generateRandomtagsSlice(tagKeyNum, tagValueNum int) []string {
	// generate original tag pairs
	FPGen := FullPermutationGen{}
	originalTagsSlice := FPGen.GenerateQueryTagsSlice(tagKeyNum, tagValueNum)

	// delete partially and repeat partially from the orginal tag pairs, 3 * 50% = 1.5
	tagsSlice := []models.Tags{}
	for i := 0; i < 3; i++ {
		for _, tags := range originalTagsSlice {
			// choose 50% tag pairs randomly
			rand.Seed(time.Now().UnixNano())
			choose := rand.Intn(2)
			if choose == 1 {
				tagsSlice = append(tagsSlice, tags)
			}
		}
	}
	// shuffle
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(tagsSlice), func(i, j int) {
		tagsSlice[i], tagsSlice[j] = tagsSlice[j], tagsSlice[i]
	})

	// serialize to []string
	tagsSliceStrings := []string{}
	for _, tags := range tagsSlice {
		tagsSlicetring := ""
		for _, tag := range tags {
			tagsSlicetring += fmt.Sprintf("%s\t%s\t", tag.Key, tag.Value)
		}
		tagsSliceStrings = append(tagsSliceStrings, tagsSlicetring)
	}
	return tagsSliceStrings
}

func (g *RandomGenerator) getFilePath(tagKeyNum, tagValueNumm int) string {
	return fmt.Sprintf("%s_%d_%d", RANDOM_GENERATOR_FILE_PATH, tagKeyNum, tagValueNumm)
}

func (g *RandomGenerator) createAndWriteToFile(tagsSlice []string, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, line := range tagsSlice {
		_, err := f.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *RandomGenerator) readtagsSliceFromFile(filePath string) []models.Tags {
	f, _ := os.Open(filePath)
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)

	tagsSlice := []models.Tags{}

	for fileScanner.Scan() {
		tagsSlice = append(tagsSlice, convertToTagPairs(fileScanner.Text()))
	}
	return tagsSlice
}

func convertToTagPairs(line string) models.Tags {
	// delete `\t` and `\n` at back
	// TODO(vinland-avalon): error handle
	line = line[:len(line)-1]
	elems := strings.Split(line, "\t")
	m := map[string]string{}
	for i := 0; i < len(elems); i += 2 {
		m[elems[i]] = elems[i+1]
	}
	return models.NewTags(m)
}
