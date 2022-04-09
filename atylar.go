// Atylar is an opinionated storage system with version history.
package atylar

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

type Store struct {
	Root       string // Path to store root
	Generation uint64
}

// getGenerationFromFileName reads the path and returns value of the number after the last `@` sign
// in the last element of the path. If there is no generation specified or there is a parsing error,
// 0 is returned.
func getGenerationFromFileName(path string) uint64 {
	if path == "" {
		return 0
	}
	path = strings.TrimRight(path, "/")
	for i := len(path) - 2; i >= 0; i-- {
		switch path[i] {
		case '/', '\\':
			return 0
		case '@':
			generation, _ := strconv.ParseUint(path[i+1:], 10, 64)
			return generation
		}
	}
	return 0
}

// New opens or creates a new store.
func New(root string) (Store, error) {
	err := os.MkdirAll(root+"/.history", 0755)
	if err != nil {
		return Store{}, err
	}
	var generation uint64 = 1
	filepath.WalkDir(root+"/.history", fs.WalkDirFunc(func(path string, _ fs.DirEntry, _ error) error {
		if g := getGenerationFromFileName(path); g > generation {
			generation = g
		}
		return nil
	}))
	// TODO
	return Store{Root: root, Generation: generation}, nil
}

func (S *Store) realPath(path string, history bool) string {
	if history {
		return filepath.Join(S.Root, ".history", filepath.Clean("/"+path))
	} else {
		return filepath.Join(S.Root, filepath.Clean("/"+path))
	}
}

var ErrIllegalPath = errors.New("atylar: illegal path")

// checkPath ensures that the given path is allowed to be used. If it
// isn't, checkPath returns ErrIllegalPath. It should be used only for
// user-defined paths, and not for auto-generated ones like history paths.
// Empty path is also illegal.
// This function is used in Store.captureHistory and so it usually doesn't
// need to be included in other functions, as these frequently use
// captureHistory.
func checkPath(path string) error {
	path = filepath.Clean(path)
	pathElements := strings.Split(path, string(filepath.Separator))
	empty := true
	for _, element := range pathElements {
		if strings.Contains(element, "@") || strings.HasPrefix(element, ".") {
			return ErrIllegalPath
		}
		if element != "" {
			empty = false
		}
	}
	if empty {
		return ErrIllegalPath
	} else {
		return nil
	}
}

// GetGeneration increments current generation if the argument is true and returns it.
func (S *Store) GetGeneration(increment bool) uint64 {
	if increment {
		return atomic.AddUint64(&S.Generation, 1)
	} else {
		return S.Generation
	}
}

// FileHistory returns generations available for the given path.
func (S *Store) FileHistory(path string) ([]uint64, error) {
	out := []uint64{}
	dir, err := os.ReadDir(filepath.Dir(S.realPath(path, true)))
	if err != nil {
		return nil, err
	}
	for _, entry := range dir {
		if n := entry.Name(); strings.HasPrefix(n, filepath.Base(path)+"@") {
			if g := getGenerationFromFileName(n); g != 0 {
				out = append(out, g)
			}
		}
	}
	// Sort found generations starting from the newest.
	sort.Slice(out, func(i, j int) bool { return out[i] > out[j] })
	return out, nil
}

// compareFiles return true if both files are equal.
// Based on https://stackoverflow.com/a/30038571
func compareFiles(file1, file2 string) (bool, error) {
	f1s, err := os.Stat(file1)
	if err != nil {
		return false, err
	}
	f2s, err := os.Stat(file2)
	if err != nil {
		return false, err
	}
	if f1s.Size() != f2s.Size() {
		return false, nil
	}

	f1, err := os.Open(file1)
	if err != nil {
		return false, err
	}
	f2, err := os.Open(file2)
	if err != nil {
		return false, err
	}

	for {
		b1 := make([]byte, 64000)
		_, err1 := f1.Read(b1)
		b2 := make([]byte, 64000)
		_, err2 := f2.Read(b2)
		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return true, nil
			} else if err1 == io.EOF || err2 == io.EOF {
				return false, nil
			} else if err1 != nil {
				return false, err1
			} else {
				return false, err2
			}
		}
		if !bytes.Equal(b1, b2) {
			return false, nil
		}
	}
}

// copy is a helper function to copy files. If overwrite flag is set
// to false and the target file exists, the file will not be copied
// and an error will be returned.
func copy(from, to string, overwrite bool) error {
	f1, err := os.Open(from)
	if err != nil {
		return err
	}
	defer f1.Close()
	err = os.MkdirAll(filepath.Dir(to), 0755)
	if err != nil {
		return err
	}
	flags := 0
	if overwrite {
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	} else {
		flags = os.O_CREATE | os.O_WRONLY | os.O_EXCL
	}
	f2, err := os.OpenFile(to, flags, 0644)
	if err != nil {
		return err
	}
	defer f2.Close()
	_, err = io.Copy(f2, f1)
	if err != nil {
		return err
	}
	return nil
}

// captureHistory backups a file. If the file doesn't exist or the current
// version is already saved, it does nothing.
func (S *Store) captureHistory(path string) error {
	if err := checkPath(path); err != nil {
		return err
	}
	currentPath := S.realPath(path, false)
	if _, err := os.Stat(currentPath); errors.Is(err, os.ErrNotExist) {
		return nil // File doesn't exist.
	}
	history, err := S.FileHistory(path)
	if err != nil {
		return err
	}
	if len(history) != 0 {
		latestPath := S.realPath(path, true) + "@" + strconv.FormatUint(history[0], 10)
		eq, err := compareFiles(currentPath, latestPath)
		if err != nil {
			return err
		}
		if eq {
			return nil // This version is already saved
		}
	}
	// Capturing
	capturePath := S.realPath(path, true) + "@" + strconv.FormatUint(S.GetGeneration(true), 10)
	copy(currentPath, capturePath, false)
	return nil
}

// Write returns a file descriptor for writing.
// If the file exists, it is truncated.
func (S *Store) Write(path string) (*os.File, error) {
	err := S.captureHistory(path)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(S.realPath(path, false), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
}

// Open opens given file for reading. If generation is non-zero, it opens a historic version.
func (S *Store) Open(path string, generation uint64) (*os.File, error) {
	if generation == 0 {
		return os.Open(S.realPath(path, false))
	} else {
		return os.Open(S.realPath(path, true) + "@" + strconv.FormatUint(generation, 10))
	}
}

// Copy copies a file.
func (S *Store) Copy(from, to string) error {
	err := S.captureHistory(to)
	if err != nil {
		return err
	}
	return copy(S.realPath(from, false), S.realPath(to, false), true)
}

// Move moves a file.
func (S *Store) Move(from, to string) error {
	err := S.captureHistory(to)
	if err != nil {
		return err
	}
	err = S.captureHistory(from)
	if err != nil {
		return err
	}
	return os.Rename(S.realPath(from, false), S.realPath(to, false))
}

// Remove removes a file.
func (S *Store) Remove(path string) error {
	err := S.captureHistory(path)
	if err != nil {
		return err
	}
	return os.Remove(S.realPath(path, false))
}

// List lists files (and not directories) in the specified directory.
// List returns paths relative to root, not to the path specified as the argument.
func (S *Store) List(path string, history bool, recursive bool) ([]string, error) {
	out := []string{}
	entries, err := os.ReadDir(S.realPath(path, history))
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Name() == ".history" {
			continue
		}
		entryPath := filepath.Join(path, entry.Name())
		if !entry.IsDir() {
			out = append(out, entryPath)
		}
		if recursive && entry.IsDir() {
			children, err := S.List(entryPath, history, true)
			if err != nil {
				return out, err
			}
			out = append(out, children...)
		}
	}
	return out, nil
}
