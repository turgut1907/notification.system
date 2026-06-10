package domain

import "testing"

func TestListFilterNormalize(t *testing.T) {
	f := ListFilter{}
	f.Normalize()
	if f.Limit != 50 {
		t.Fatalf("default limit %d", f.Limit)
	}
	f = ListFilter{Limit: 500}
	f.Normalize()
	if f.Limit != 200 {
		t.Fatalf("max limit %d", f.Limit)
	}
}
