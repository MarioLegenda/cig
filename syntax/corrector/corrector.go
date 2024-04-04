package corrector

import (
	"cig/syntax/splitter"
	"errors"
	"fmt"
	"os"
	"strings"
)

var InvalidNumberOfChunks = errors.New("Invalid number of chunks. Minimum number of syntax chunks is 6.")
var InvalidSelectChunk = errors.New("Expected 'select', got something else.")
var InvalidAsChunk = errors.New("Expected 'as', got something else.")
var InvalidFromChunk = errors.New("Expected 'from', got something else.")
var InvalidFilePathChunk = errors.New("Expected 'path:path_to_file' but did not get the path: part")
var InvalidFilePath = errors.New("Invalid file path.")

func IsShallowSyntaxCorrect(s splitter.Splitter) []error {
	chunks := s.Chunks()
	errs := make([]error, 0)

	if len(chunks) < 6 {
		errs = append(errs, InvalidNumberOfChunks)
		return errs
	}

	if strings.ToLower(chunks[0]) != "select" {
		errs = append(errs, InvalidSelectChunk)
	}

	if strings.ToLower(chunks[2]) != "from" {
		errs = append(errs, InvalidFromChunk)
	}

	splitPath := strings.Split(chunks[3], ":")
	if len(splitPath) != 2 {
		errs = append(errs, InvalidFilePathChunk)
		return errs
	}

	if splitPath[0] != "path" {
		errs = append(errs, InvalidFilePathChunk)
		return errs
	}

	path := splitPath[1]
	stat, err := os.Stat(path)
	if err != nil {
		errs = append(errs, fmt.Errorf("File path %s does not exist: %w", path, InvalidFilePath))
		return errs
	}

	nameSplit := strings.Split(stat.Name(), ".")
	if nameSplit[1] != "csv" {
		errs = append(errs, fmt.Errorf("File %s is not a csv file or it does not have a csv extension: %w", path, InvalidFilePath))
	}

	if strings.ToLower(chunks[4]) != "as" {
		errs = append(errs, InvalidAsChunk)
	}

	return errs
}
