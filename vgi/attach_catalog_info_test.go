// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import "testing"

func TestSerializeAttachCatalogInfo(t *testing.T) {
	info := AttachCatalogInfo{
		Alias:     "acme_lake",
		Target:    "ducklake:sqlite:/data/meta.sqlite",
		DBType:    "",
		Options:   map[string]string{"DATA_PATH": "/data/"},
		Hidden:    true,
		Required:  true,
		SecretRef: "pg",
	}
	b, err := SerializeAttachCatalogInfo(info)
	if err != nil {
		t.Fatalf("SerializeAttachCatalogInfo: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("SerializeAttachCatalogInfo returned empty bytes")
	}
}

func TestSerializeScanBranchCatalogTable(t *testing.T) {
	cat, sch, tbl, filter := "acme_lake", "main", "events", "id < 100"
	branch := &ScanBranch{
		FunctionName:  "", // catalog-table branch: empty function name
		BranchFilter:  &filter,
		SourceCatalog: &cat,
		SourceSchema:  &sch,
		SourceTable:   &tbl,
	}
	b, err := SerializeScanBranch(branch)
	if err != nil {
		t.Fatalf("SerializeScanBranch (catalog-table): %v", err)
	}
	if len(b) == 0 {
		t.Fatal("SerializeScanBranch returned empty bytes")
	}
}
