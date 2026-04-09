package config

import "testing"

func TestParseIndexYAMLType(t *testing.T) {
	for _, c := range []struct {
		in      string
		wantLoc bool
		wantErr bool
	}{
		{"", false, false},
		{"global", false, false},
		{"GLOBAL", false, false},
		{"local", true, false},
		{" Local ", true, false},
		{"other", false, true},
	} {
		loc, err := ParseIndexYAMLType(c.in)
		if c.wantErr {
			if err == nil {
				t.Fatalf("in %q expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("in %q: %v", c.in, err)
		}
		if loc != c.wantLoc {
			t.Fatalf("in %q: got local=%v want %v", c.in, loc, c.wantLoc)
		}
	}
}

func TestFormatIndexYAMLType(t *testing.T) {
	if FormatIndexYAMLType(false) != "global" || FormatIndexYAMLType(true) != "local" {
		t.Fatal()
	}
}
