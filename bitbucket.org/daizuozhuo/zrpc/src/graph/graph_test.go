package graph

import (
    "testing"
    "io/ioutil"
    "os"
)

func TestSplitGraph(t *testing.T) {
    outFilePaths, err := SplitGraph("test/imagelist.txt", "test/option.txt", 2)
    if err != nil {
        t.Error("split graph errer", err)
    }

    expect := []string{"1.jpg\n4.jpg\n", "2.jpg\n3.jpg\n"}
    for i, outFilePath := range outFilePaths {
        outFile, err := ioutil.ReadFile(outFilePath)
        if err != nil {
            t.Error("cannot read file", outFilePath, err)
        }
        result := string(outFile)
        if result != expect[i] {
            t.Errorf("expect to get %s, got %s", expect[i], result)
        }
    }
    for _, outFilePath := range outFilePaths {
        os.Remove(outFilePath)
    }
}
