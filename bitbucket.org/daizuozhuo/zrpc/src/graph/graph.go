package graph

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func MapName(fileName string, index int) string {
	return fmt.Sprintf("%s-%d", fileName, index)
}

func Split(fileName string, N int) ([]string, error) {
	infile, err := os.Open(fileName)
	if err != nil {
		log.Println("Error: split txt file", err)
		return nil, err
	}
	defer infile.Close()
	var lines []string
	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	outFiles := make([]string, N)
	step := len(lines)/N + 1
	i := 0
	for start := 0; start < len(lines); start += step {
		outName := MapName(fileName, i)
		if _, err := os.Stat(outName); err == nil {
			log.Printf("Info: %s exists; remove it", outName)
			os.Remove(outName)
		}
		outFile, err := os.Create(outName)
		if err != nil {
			log.Print("Error: create output file", outFile, err)
			return nil, err
		}
		outFiles[i] = outName
		writer := bufio.NewWriter(outFile)
		for j := start; j < len(lines) && j < start+step; j++ {
			writer.WriteString(lines[j] + "\n")
		}
		writer.Flush()
		outFile.Close()
		i++
	}
	return outFiles, nil
}

func SplitGraph(inputFilePath, optionFilePath string, N int) ([]string, error) {
	return Split(inputFilePath, N)
}
