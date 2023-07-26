package atylar

import (
	"testing"
)

func TestNormalizeName(t *testing.T) {
	tests := []struct {
		in         string
		out        string
		outHistory string // when history=true, may be empty if the output should be the same
	}{
		{"", "", ""},
		{"/abc/def", "abc_def", ""},
		{"abc/def", "abc_def", ""},
		{"abc-def/", "abc-def", ""},
		{".hidden", "hidden", ""},
		{"abc@12", "abc_12", "abc@12"},
		{"ab@b@3", "ab_b_3", "ab_b@3"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if n := normalizeName(tt.in, false); n != tt.out {
				t.Error("Got", n, "but expected", tt.out)
			}
			n := normalizeName(tt.in, true)
			out := tt.outHistory
			if out == "" {
				out = tt.out
			}
			if n != out {
				t.Error("Got", n, "but expected", out, "(history)")
			}
		})
	}
}

func TestGeneration(t *testing.T) {
	tests := []struct {
		in  string
		out uint64
	}{
		{"", 0},
		{"abc", 0},
		{"@", 0},
		{"a@", 0},
		{"abcdefgh@", 0},
		{"145", 0},
		{"@1", 1},
		{"@324", 324},
		{"a@165", 165},
		{"abcdefgh@431", 431},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if g := generation(tt.in); g != tt.out {
				t.Error("Got", g, "but expected", tt.out)
			}
		})
	}
}

func TestBaseName(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{"", ""},
		{"abc", "abc"},
		{"@", ""},
		{"a@", "a"},
		{"abcdefgh@", "abcdefgh"},
		{"145", "145"},
		{"@1", ""},
		{"@324", ""},
		{"a@165", "a"},
		{"abcdefgh@431", "abcdefgh"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if g := baseName(tt.in); g != tt.out {
				t.Error("Got", g, "but expected", tt.out)
			}
		})
	}
}
