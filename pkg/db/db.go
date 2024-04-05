package db

import "cig/pkg/syntax"

type db struct {
	structure syntax.Structure
}

type DB interface {
	Run() (map[string]string, error)
}

func (d *db) Run() (map[string]string, error) {
	return nil, nil
}

func New(s syntax.Structure) DB {
	return &db{structure: s}
}
