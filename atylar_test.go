package atylar

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestGetGenerationFromFileName(t *testing.T) {
	tests := []struct {
		in  string
		out uint64
	}{
		{"", 0},
		{"@", 0},
		{"a@", 0},
		{"abcdefgh@", 0},
		{"abcde/abcdefgh@", 0},
		{"145", 0},
		{"@1", 1},
		{"@324", 324},
		{"a@165", 165},
		{"abcdefgh@431", 431},
		{"abcde/abcdefgh@975", 975},
		{"abcde@431/abcdefgh@975", 975},
		{"abcde@431/abcdefgh@", 0},
		{"abcde@431/ab@cdefgh", 0},
		{"abcde@431/abcdefgh", 0},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if g := getGenerationFromFileName(tt.in); g != tt.out {
				t.Error("Got", g, "but expected", tt.out)
			}
		})
	}
}

func TestRealPath(t *testing.T) {
	tests := []struct {
		root, path string
		history    bool
		out        string
	}{
		{"/path/to/store", "/simple/path", false, "/path/to/store/simple/path"},
		{"/path/to/store", "simple/path", false, "/path/to/store/simple/path"},
		{"/path/to/store", "../../try/to/escape", false, "/path/to/store/try/to/escape"},
		{"/path/to/store", "more/../..//../complicated", false, "/path/to/store/complicated"},
		{"/path/to/store", "/../../try/to/escape", false, "/path/to/store/try/to/escape"},
		{"/path/to/store", "/more/../..//../complicated", false, "/path/to/store/complicated"},
		{"/path/to/store", "/simple/path", true, "/path/to/store/.history/simple/path"},
		{"/path/to/store", "simple/path", true, "/path/to/store/.history/simple/path"},
		{"/path/to/store", "../../try/to/escape", true, "/path/to/store/.history/try/to/escape"},
		{"/path/to/store", "more/../..//../complicated", true, "/path/to/store/.history/complicated"},
	}
	for _, tt := range tests {
		t.Run(tt.root+"+"+tt.path, func(t *testing.T) {
			s := Store{Root: tt.root}
			if r := s.realPath(tt.path, tt.history); r != tt.out {
				t.Error("Got", r, "but expected", tt.out)
			}
		})
	}
}

func TestStore(t *testing.T) {
	t.Run("Simple creation", func(t *testing.T) {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
			return
		}
		if s.Generation != 1 {
			t.Error("Expected generation to be equal 1, but got", s.Generation)
		}
	})
	t.Run("Infer generation", func(t *testing.T) {
		d := t.TempDir()
		err := os.MkdirAll(filepath.Join(d, ".history", "dir"), 0755)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(filepath.Join(d, ".history", "dir", "file@123"))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		s, err := New(d)
		if err != nil {
			t.Fatal(err)
		}
		if s.Generation != 123 {
			t.Error("Expected generation to be equal 123, but got", s.Generation)
		}
	})
	t.Run("Clean structure", func(t *testing.T) {
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
			return
		}
		if _, err := os.Stat(s.realPath("empty", false)); err == nil {
			t.Fatal("This directory shouldn't exist")
		}
	})
}

// prepareComplexTest prepares a mock store directory for further tests.
// Directory path is returned.
func prepareComplexTest(t *testing.T) string {
	d := t.TempDir()
	err := os.MkdirAll(filepath.Join(d, ".history", "dir"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(d, "dir"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(d, "dir", "dir2"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(d, "empty", "directory"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	{ // Pre-existing history
		f, err := os.Create(filepath.Join(d, ".history", "dir", "file@123"))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	{ // File 1
		f, err := os.Create(filepath.Join(d, "dir", "file"))
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
		f, err := os.Create(filepath.Join(d, "dir", "file2"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("Hello from the second file!")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	{ // File 3
		f, err := os.Create(filepath.Join(d, "dir", "dir2", "file3"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("Hello from the third file!")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	return d
}

func TestList(t *testing.T) {
	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		path               string
		history, recursive bool
		output             string
	}{
		{"", false, true, "[dir/dir2/file3 dir/file dir/file2]"},
		{"", false, false, "[]"},
		{"", true, true, "[dir/file@123]"},
		{"dir/", false, true, "[dir/dir2/file3 dir/file dir/file2]"},
		{"dir/dir2/", false, true, "[dir/dir2/file3]"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt), func(t *testing.T) {
			l, err := s.List(tt.path, tt.history, tt.recursive)
			if err != nil {
				t.Error("Error listing directory:", err)
			}
			out := fmt.Sprintf("%v", l)
			if out != tt.output {
				t.Error("Expected:\n" + tt.output + "\nReceived:\n" + out)
			}
		})
	}
}

func TestCheckPath(t *testing.T) {
	tests := []struct {
		path  string
		legal bool
	}{
		{"simple/path", true},
		{"/simple/path", true},
		{"/file/with/extension.txt", true},
		{"/doubled//slash", true},
		{"", false},
		{".", false},
		{"..", false},
		{"/", false},
		{"/////", false},
		{"a/hidden/.file", false},
		{"a/.hidden/directory", false},
		{"file/with/version@123", false},
		{"directory/with/version@123/number", false},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			err := checkPath(tt.path)
			if (err != nil) == tt.legal {
				expected := "nil,"
				if !tt.legal {
					expected = "an error,"
				}
				t.Error("Expected checkPath to return", expected, "but received:", err)
			}
		})
	}
}

func TestFileHistory(t *testing.T) {
	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		path string
	}{
		{"dir/file"},
		{"/dir/file"},
		{"/dir/file/"},
		{"/dir//file/"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			v, err := s.FileHistory(tt.path)
			if err != nil {
				t.Fatal(err)
			}
			if len(v) != 1 || v[0] != 123 {
				t.Fatal("Expected [123], received", v)
			}
		})
	}
}

func TestCompareFiles(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "f1"), []byte("File 1."), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "f2"), []byte("Exact file."), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "f3"), []byte("Exact file."), 0644); err != nil {
		t.Fatal(err)
	}

	if eq, err := compareFiles(filepath.Join(d, "f1"), filepath.Join(d, "f2")); err != nil {
		t.Fatal(err)
	} else if eq {
		t.Error("Files were different, but function returned true.")
	}

	if eq, err := compareFiles(filepath.Join(d, "f2"), filepath.Join(d, "f3")); err != nil {
		t.Fatal(err)
	} else if !eq {
		t.Error("Files were equal, but function returned false.")
	}

	if eq, err := compareFiles(filepath.Join(d, "f3"), filepath.Join(d, "f3")); err != nil {
		t.Fatal(err)
	} else if !eq {
		t.Error("Both paths pointed to the same file, but function returned false.")
	}
}

func TestRawCopy(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "f1"), []byte("File."), 0644); err != nil {
		t.Fatal(err)
	}

	if err := copy(filepath.Join(d, "f1"), filepath.Join(d, "f2"), false); err != nil {
		t.Fatal(err)
	}

	if eq, err := compareFiles(filepath.Join(d, "f1"), filepath.Join(d, "f2")); err != nil {
		t.Fatal(err)
	} else if !eq {
		t.Error("Copy is different than the source.")
	}

	if err := os.WriteFile(filepath.Join(d, "f1"), []byte("Now testing overwriting."), 0644); err != nil {
		t.Fatal(err)
	}

	if err := copy(filepath.Join(d, "f1"), filepath.Join(d, "f2"), false); err == nil {
		t.Error("Overwrite was set to false, but function didn't return an error.")
	}

	if eq, err := compareFiles(filepath.Join(d, "f1"), filepath.Join(d, "f2")); err != nil {
		t.Fatal(err)
	} else if eq {
		t.Error("Function overwrited the target, when overwrite was set to false.")
	}

	if err := copy(filepath.Join(d, "f1"), filepath.Join(d, "f2"), true); err != nil {
		t.Error(err)
	}

	if eq, err := compareFiles(filepath.Join(d, "f1"), filepath.Join(d, "f2")); err != nil {
		t.Fatal(err)
	} else if !eq {
		t.Error("Copy is different than the source.")
	}
}

func TestCaptureHistory(t *testing.T) {
	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}

	err = s.captureHistory("dir/file")
	if err != nil {
		t.Error(err)
	}

	if s.Generation != 124 {
		t.Error("Generation was not incremented, got", s.Generation, "instead of", 124)
	}

	v, err := s.FileHistory("dir/file")
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 || v[0] != 124 || v[1] != 123 {
		t.Log(s.List("dir/", true, true))
		t.Error("Got", v, "when checking history, but expected [124 123]")
	}

	if eq, err := compareFiles(s.realPath("dir/file", false), s.realPath("dir/file", true)+"@124"); err != nil {
		t.Fatal(err)
	} else if !eq {
		t.Error("Captured file has different content.")
	}
}

func TestWrite(t *testing.T) {
	// Testing history

	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}

	fd, err := s.Write("dir/file")
	if err != nil {
		t.Fatal(err)
	}
	fd.Close()

	v, err := s.FileHistory("dir/file")
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 || v[0] != 124 || v[1] != 123 {
		t.Log(s.List("dir/", true, true))
		t.Error("Got", v, "when checking history, but expected [124 123]")
	}
}

func TestOpen(t *testing.T) {
	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}

	fd, err := s.Open("dir/file", 0)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatal(err)
	}
	if string(c) != "Hello!" {
		t.Error("Expected file to contain \"Hello!\", but got:", string(c))
	}
	fd.Close()

	fd, err = s.Open("dir/file", 123)
	if err != nil {
		t.Fatal(err)
	}
	c, err = ioutil.ReadAll(fd)
	if err != nil {
		t.Fatal(err)
	}
	if string(c) != "" {
		t.Error("Expected file to be empty, but got:", string(c))
	}
	fd.Close()
}

func TestCopy(t *testing.T) {
	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}

	err = s.Copy("dir/file", "dir/file2")
	if err != nil {
		t.Error(err)
	}

	err = s.Copy("dir/file", "dir/file5")
	if err != nil {
		t.Error(err)
	}

	fd, err := s.Open("dir/file2", 0)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatal(err)
	}
	if string(c) != "Hello!" {
		t.Error("Expected file to contain \"Hello!\", but got:", string(c))
	}
	fd.Close()

	v, err := s.FileHistory("dir/file2")
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 1 || v[0] != 124 {
		t.Log(s.List("dir/", true, true))
		t.Error("Got", v, "when checking history, but expected [124]")
	}

	fd, err = s.Open("dir/file5", 0)
	if err != nil {
		t.Fatal(err)
	}
	c, err = ioutil.ReadAll(fd)
	if err != nil {
		t.Fatal(err)
	}
	if string(c) != "Hello!" {
		t.Error("Expected file to contain \"Hello!\", but got:", string(c))
	}
	fd.Close()
}

func TestMove(t *testing.T) {
	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}

	err = s.Move("dir/file", "dir/file2")
	if err != nil {
		t.Error(err)
	}

	err = s.Move("dir/file", "dir/file5")
	if err == nil {
		t.Error("Moving the same file twice produced no error.")
	}

	fd, err := s.Open("dir/file2", 0)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatal(err)
	}
	if string(c) != "Hello!" {
		t.Error("Expected file to contain \"Hello!\", but got:", string(c))
	}
	fd.Close()

	v, err := s.FileHistory("dir/file2")
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 1 || v[0] != 124 {
		t.Log(s.List("dir/", true, true))
		t.Error("Got", v, "when checking history, but expected [124]")
	}
}

func TestRemove(t *testing.T) {
	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}

	err = s.Remove("dir/file")
	if err != nil {
		t.Error(err)
	}

	_, err = s.Open("dir/file", 0)
	if err == nil {
		t.Error("Opening a removed file produced no error.")
	}

	v, err := s.FileHistory("dir/file")
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 || v[0] != 124 || v[1] != 123 {
		t.Log(s.List("dir/", true, true))
		t.Error("Got", v, "when checking history, but expected [124 123]")
	}
}

func TestCleanDirectory(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "stop"), []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(d, "a/very/long/path/to/a/directory"), 0755); err != nil {
		t.Fatal(err)
	}

	if err := cleanDirectory(filepath.Join(d, "a/very/long/path/to/a/directory")); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(d, "stop")); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatal("This file should exist")
		}
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(d, "a")); err == nil {
		t.Fatal("This directory shouldn't exist")
	}
}

func TestCleanDirectoryStructure(t *testing.T) {
	d := t.TempDir()
	if err := os.MkdirAll(filepath.Join(d, "a/very/long/path/to/a/directory"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(d, "a/very/long/path/to/another/directory"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(d, "a/very/short/path"), 0755); err != nil {
		t.Fatal(err)
	}

	if err := cleanDirectoryStructure(d); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(d); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatal("This directory should exist")
		}
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(d, "a")); err == nil {
		t.Fatal("This directory shouldn't exist")
	}
}

func TestFixPath(t *testing.T) {
	tests := []struct {
		path string
	}{
		{"simple/path"},
		{"/simple/path"},
		{"/file/with/extension.txt"},
		{"/doubled//slash"},
		{"a/hidden/.file"},
		{"a/.hidden/directory"},
		{"file/with/version@123"},
		{"directory/with/version@123/number"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			err := checkPath(tt.path)
			if err == nil {
				p := fixPath(tt.path)
				err = checkPath(p)
				if err != nil {
					t.Error("Path was broken (" + tt.path + " -> " + p + ")")
				}
				if p != filepath.Clean(tt.path) {
					t.Error("Path was significantly changed (" + filepath.Clean(tt.path) + " -> " + p + ")")
				}
			} else {
				p := fixPath(tt.path)
				err = checkPath(p)
				if err != nil {
					t.Error("Path wasn't fixed (" + tt.path + " -> " + p + ")")
				}
			}
		})
	}
}

func TestStat(t *testing.T) {
	s, err := New(prepareComplexTest(t))
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := s.Stat("dir", false); err != nil {
		t.Error(err)
	} else if !fi.IsDir() {
		t.Error("IsDir returned false, but path pointed to a directory.")
	}
	if fi, err := s.Stat("dir/file", false); err != nil {
		t.Error(err)
	} else if fi.IsDir() {
		t.Error("IsDir returned true, but path pointed to a file.")
	}
}
