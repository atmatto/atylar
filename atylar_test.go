package atylar

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestNormalizeName(t *testing.T) {
	tests := []struct {
		in         string
		out        string
		outHistory string // when history=true; may be empty if the output should be the same as usual
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

func TestFilePath(t *testing.T) {
	S := Store{"/tmp/dir/", 42}
	tests := []struct {
		in         string
		out        string
		outHistory string
	}{
		{"file", "/tmp/dir/file", "/tmp/dir/.history/file"},
		{"../file", "/tmp/dir/file", "/tmp/dir/.history/file"},
		{"../../file", "/tmp/dir/file", "/tmp/dir/.history/file"},
		{"file/../../", "/tmp/dir", "/tmp/dir/.history"},
		{"dir/file", "/tmp/dir/dir_file", "/tmp/dir/.history/dir_file"},
		{"file@1", "/tmp/dir/file_1", "/tmp/dir/.history/file_1"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if p := S.filePath(tt.in, false); p != tt.out {
				t.Error("Got", p, "but expected", tt.out)
			}
		})
		t.Run(tt.in+" (history)", func(t *testing.T) {
			if p := S.filePath(tt.in, true); p != tt.outHistory {
				t.Error("Got", p, "but expected", tt.outHistory)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	d := t.TempDir()
	err := os.MkdirAll(filepath.Join(d, ".history"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	{ // Pre-existing history
		f, err := os.Create(filepath.Join(d, ".history", "file@123"))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	{ // File 1
		f, err := os.Create(filepath.Join(d, "file"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("Hello!")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	{ // File 2
		f, err := os.Create(filepath.Join(d, "file@2"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("Hello from the second file!")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	S := Store{d, 123}
	if err := S.normalize(); err != nil {
		t.Fatal(err)
	}

	out := d + ":\nfile\nfile_2\n.history/\n\n" + d + "/.history:\nfile@123\n"
	cmd := exec.Command("ls", "-RAp", d)
	if bytes, err := cmd.Output(); err != nil {
		t.Fatal(err)
	} else {
		if string(bytes) != out {
			t.Errorf("Got:\n%s\nbut expected:\n%s", string(bytes), out)
		}
	}
}

// createMockStore returns the path to a directory
// containing a sample store for further tests
func createMockStore(t *testing.T) string {
	d := t.TempDir()
	err := os.MkdirAll(filepath.Join(d, ".history"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	{ // Pre-existing history
		f, err := os.Create(filepath.Join(d, ".history", "file@123"))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	{ // File 1
		f, err := os.Create(filepath.Join(d, "file"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("Hello!")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	{ // File 2
		f, err := os.Create(filepath.Join(d, "file2"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("Hello from the second file!")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	return d
}

func TestInitGeneration(t *testing.T) {
	t.Run("Empty history", func(t *testing.T) {
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
			t.Error(err)
		}
		S := Store{d, 0}
		if err := S.initGeneration(); err != nil {
			t.Error(err)
		}
		if S.Generation != 0 {
			t.Error("Got", S.Generation, "but expected", 0)
		}
	})
	t.Run("Normal", func(t *testing.T) {
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc@12"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc@14"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		S := Store{d, 0}
		if err := S.initGeneration(); err != nil {
			t.Error(err)
		}
		if S.Generation != 14 {
			t.Error("Got", S.Generation, "but expected", 14)
		}
	})
	t.Run("Missing generation", func(t *testing.T) {
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		S := Store{d, 0}
		if err := S.initGeneration(); err != nil {
			t.Error(err)
		}
		if S.Generation != 0 {
			t.Error("Got", S.Generation, "but expected", 0)
		}
	})
	t.Run("Malformed generation", func(t *testing.T) {
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc@jkl"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		S := Store{d, 0}
		if err := S.initGeneration(); err != nil {
			t.Error(err)
		}
		if S.Generation != 0 {
			t.Error("Got", S.Generation, "but expected", 0)
		}
	})
}

func TestGetGeneration(t *testing.T) {
	d := t.TempDir()
	S := Store{d, 0}
	if g := S.GetGeneration(false); g != 0 {
		t.Error("Got", g, "but expected", 0)
	}
	if g := S.GetGeneration(false); g != 0 {
		t.Error("Got", g, "but expected", 0)
	}
	if g := S.GetGeneration(true); g != 1 {
		t.Error("Got", g, "but expected", 1)
	}
	if g := S.GetGeneration(false); g != 1 {
		t.Error("Got", g, "but expected", 1)
	}
	if g := S.GetGeneration(true); g != 2 {
		t.Error("Got", g, "but expected", 2)
	}
}

func TestHistory(t *testing.T) {
	t.Run("Nonexistent", func(t *testing.T) {
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
			t.Error(err)
		}
		S := Store{d, 0}
		h, err := S.History("abc")
		if err != nil || len(h) != 0 {
			t.Error("Expected [] <nil> but got", h, err)
		}
	})
	t.Run("Normal", func(t *testing.T) {
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc@12"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc@14"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		S := Store{d, 0}
		h, err := S.History("abc")
		if err != nil || len(h) != 2 || h[0] != 14 || h[1] != 12 {
			t.Error("Expected [14 12] <nil> but got", h, err)
		}
	})
	t.Run("Missing generation", func(t *testing.T) {
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc@12"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		S := Store{d, 0}
		h, err := S.History("abc")
		if err != nil || len(h) != 1 || h[0] != 12 {
			t.Error("Expected [12] <nil> but got", h, err)
		}
	})
	t.Run("Malformed generation", func(t *testing.T) {
		d := t.TempDir()
		if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(d, ".history", "abc@jkl"), []byte{}, 0644); err != nil {
			t.Error(err)
		}
		S := Store{d, 0}
		h, err := S.History("abc")
		if err != nil || len(h) != 0 {
			t.Error("Expected [] <nil> but got", h, err)
		}
	})
}

func TestRecordHistory(t *testing.T) {
	d := t.TempDir()
	if err := os.Mkdir(filepath.Join(d, ".history"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "abc"), []byte("v1"), 0644); err != nil {
		t.Fatal(err)
	}
	S := Store{d, 0}
	if err := S.recordHistory("abc"); err != nil {
		t.Fatal(err)
	}
	// abc@1 : v1
	if err := os.WriteFile(filepath.Join(d, "abc"), []byte("v2"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := S.recordHistory("abc"); err != nil {
		t.Fatal(err)
	}
	// abc@1 : v1, abc@2 : v2
	if err := S.recordHistory("abc"); err != nil {
		t.Fatal(err)
	}
	// abc@1 : v1, abc@2 : v2

	dir, err := os.ReadDir(filepath.Join(d, ".history"))
	if err != nil {
		t.Fatal(err)
	}
	if len(dir) != 2 || dir[0].Name() != "abc@1" || dir[1].Name() != "abc@2" {
		t.Log("Got:")
		for _, e := range dir {
			t.Log(e.Name())
		}
		t.Log("But expected [abc@1 abc@2]")
		t.Fail()
	}
	if b, err := os.ReadFile(filepath.Join(d, ".history", "abc@1")); err != nil || string(b) != "v1" {
		t.Error("Expected v1 <nil> but got", string(b), err)
	}
	if b, err := os.ReadFile(filepath.Join(d, ".history", "abc@2")); err != nil || string(b) != "v2" {
		t.Error("Expected v2 <nil> but got", string(b), err)
	}
}

func TestAtylar(t *testing.T) {
	d := t.TempDir()
	S, err := New(filepath.Join(d, "test"))
	if err != nil {
		t.Fatal(err)
	}
	if S.Directory != filepath.Join(d, "test") {
		t.Error("Expected S.Directory to be", filepath.Join(d, "test"), "but it is", S.Directory)
	}
	if S.Generation != 0 {
		t.Error("Expected S.Generation to be", 0, "but it is", S.Generation)
	}

	f, err := S.Overwrite("abc")
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
		f.WriteString("v1")
	}

	f, err = S.Overwrite("abc")
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
		f.WriteString("v2")
	}

	f, err = S.Open("abc", 0)
	if err != nil {
		t.Error(err)
	} else {
		b, err := io.ReadAll(f)
		if err != nil {
			t.Error(err)
		} else if string(b) != "v2" {
			t.Error("Expected v2 but got", string(b))
		}
	}

	f, err = S.Open("abc", 1)
	if err != nil {
		t.Error(err)
	} else {
		b, err := io.ReadAll(f)
		if err != nil {
			t.Error(err)
		} else if string(b) != "v1" {
			t.Error("Expected v1 but got", string(b))
		}
	}

	f, err = S.Overwrite("abc")
	if err != nil {
		t.Error(err)
	} else {
		defer f.Close()
		f.WriteString("v3")
	}

	f, err = S.Open("abc", 0)
	if err != nil {
		t.Error(err)
	} else {
		b, err := io.ReadAll(f)
		if err != nil {
			t.Error(err)
		} else if string(b) != "v3" {
			t.Error("Expected v3 but got", string(b))
		}
	}

	f, err = S.Open("abc", 1)
	if err != nil {
		t.Error(err)
	} else {
		b, err := io.ReadAll(f)
		if err != nil {
			t.Error(err)
		} else if string(b) != "v1" {
			t.Error("Expected v1 but got", string(b))
		}
	}

	f, err = S.Open("abc", 2)
	if err != nil {
		t.Error(err)
	} else {
		b, err := io.ReadAll(f)
		if err != nil {
			t.Error(err)
		} else if string(b) != "v2" {
			t.Error("Expected v2 but got", string(b))
		}
	}

	S, err = New(filepath.Join(d, "test"))
	if err != nil {
		t.Fatal(err)
	}
	if S.Generation != 2 {
		t.Error("Expected S.Generation to be", 2, "but it is", S.Generation)
	}

	err = S.Remove("abc")
	if err != nil {
		t.Error(err)
	}

	f, err = S.Open("abc", 3)
	if err != nil {
		t.Error(err)
	} else {
		b, err := io.ReadAll(f)
		if err != nil {
			t.Error(err)
		} else if string(b) != "v3" {
			t.Error("Expected v3 but got", string(b))
		}
	}

	_, err = S.Open("abc", 0)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Error("File shouldn't exist but the error was", err)
	}
}

// TODO: copy, move, stat, list; and optionally: new, overwrite, open, remove
