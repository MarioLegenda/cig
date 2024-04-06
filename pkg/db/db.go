package db

import (
	"cig/pkg/syntax"
	"fmt"
)

type db struct {
	structure syntax.Structure
}

type DB interface {
	Run() (map[string]string, error)
}

func (d *db) Run() (map[string]string, error) {
	file := d.structure.FileDB()
	fmt.Println(file.Alias(), file.Path())

	return nil, nil
}

func New(s syntax.Structure) DB {
	return &db{structure: s}
}
