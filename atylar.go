// Package atylar is an opinionated file storage system with version history.
// It uses a flat directory structure (no subdirectories). To start, initialize a new Store using the function New(),
// supplying the store's root directory path as the argument. All functions which may be used to modify the files
// automatically copy the current file to the `.history` directory in the current store. Historic versions are marked
// with an @ sign and the version number after the file name. The numbers are designated based on the generation,
// an always-increasing counter characteristic for the store.
package atylar

// TODO:
// Handle concurrency problems.
// Garbage collect old file versions.
// Write tests.

import (
	"bytes"
	"errors"
	"fmt"
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
	Directory  string // Path to store root
	Generation uint64 // Used to set files' versions
}

// normalizeName turns the filename into a normalized file name.
// If `history` is true, the `@` character before the version number is preserved.
func normalizeName(filename string, history bool) (normalized string) {
	normalized = strings.Trim(filepath.Clean(filename), "/\\.")
	normalized = strings.ReplaceAll(normalized, "/", "_")
	normalized = strings.ReplaceAll(normalized, "\\", "_")
	normalized = strings.ReplaceAll(normalized, string(filepath.Separator), "_")
	if history {
		c := strings.Count(normalized, "@")
		return strings.Replace(normalized, "@", "_", c-1)
	}
	return strings.ReplaceAll(normalized, "@", "_")
}

// normalize ensures that all file names are normalized.
func (S *Store) normalize() error {
	dir, err := os.ReadDir(filepath.Join(S.Directory, ".history"))
	if err != nil {
		return fmt.Errorf("normalize %s: %w", S.Directory, err)
	}
	for _, entry := range dir {
		norm := normalizeName(entry.Name(), true)
		if norm != entry.Name() {
			if err = os.Rename(filepath.Join(S.Directory, ".history", entry.Name()), filepath.Join(S.Directory, ".history", norm)); err != nil {
				return fmt.Errorf("normalize %s: %w", S.Directory, err)
			}
		}
	}

	dir, err = os.ReadDir(S.Directory)
	if err != nil {
		return fmt.Errorf("normalize %s: %w", S.Directory, err)
	}
	for _, entry := range dir {
		norm := normalizeName(entry.Name(), false)
		if norm != entry.Name() && entry.Name() != ".history" {
			if err = os.Rename(filepath.Join(S.Directory, entry.Name()), filepath.Join(S.Directory, norm)); err != nil {
				return fmt.Errorf("normalize %s: %w", S.Directory, err)
			}
		}
	}

	return nil
}

// generation reads the file name and returns value of the number after the last `@` sign.
// If there is no generation specified or there is a parsing error, 0 is returned.
func generation(filename string) (generation uint64) {
	if filename == "" {
		return 0
	}
	filename = strings.TrimRight(filename, "/")
	for i := len(filename) - 2; i >= 0; i-- {
		switch filename[i] {
		case '/', '\\':
			return 0
		case '@':
			generation, _ = strconv.ParseUint(filename[i+1:], 10, 64)
			return
		}
	}
	return
}

// baseName strips version info (text after `@`) from the file name.
func baseName(filename string) string {
	return strings.Split(filename, "@")[0]
}

// initGeneration sets the generation to the maximal present
// in the .history directory.
func (S *Store) initGeneration() error {
	dir, err := os.ReadDir(S.Directory + "/.history")
	if err != nil {
		return fmt.Errorf("initGeneration %s: %w", S.Directory, err)
	}
	for _, entry := range dir {
		if g := generation(entry.Name()); g > S.Generation {
			S.Generation = g
		}
	}
	return nil
}

// GetGeneration increments current generation if the argument is true and returns it.
func (S *Store) GetGeneration(increment bool) uint64 {
	if increment {
		return atomic.AddUint64(&S.Generation, 1)
	} else {
		return S.Generation
	}
}

// New opens or creates a new store.
func New(root string) (Store, error) {
	S := Store{Directory: root, Generation: 1}
	if err := os.MkdirAll(root, 0755); err != nil {
		return S, fmt.Errorf("new: %w", err)
	}
	if err := os.MkdirAll(root+"/.history", 0755); err != nil {
		return S, fmt.Errorf("new: %w", err)
	}
	if err := S.normalize(); err != nil {
		return S, fmt.Errorf("new: %w", err)
	}
	if err := S.initGeneration(); err != nil {
		return S, fmt.Errorf("new: %w", err)
	}
	return S, nil
}

// filePath returns the filesystem path to the file with the given name.
// If `history` is true, then the path will point to the file in the
// history directory, but a generation number needs to be appended to it
// for it to be useful. The file name is normalized.
func (S *Store) filePath(name string, history bool) string {
	if history {
		return filepath.Join(S.Directory, ".history", normalizeName(name, true))
	} else {
		return filepath.Join(S.Directory, normalizeName(name, false))
	}
}

// History returns generations available for the given file.
// The name is normalized
func (S *Store) History(file string) ([]uint64, error) {
	generations := []uint64{}
	file = normalizeName(file, false)
	dir, err := os.ReadDir(filepath.Join(S.Directory, ".history"))
	if err != nil {
		return generations, fmt.Errorf("history %s: %w", file, err)
	}
	for _, entry := range dir {
		if n := entry.Name(); strings.HasPrefix(n, filepath.Base(file)+"@") {
			if g := generation(n); g != 0 {
				generations = append(generations, g)
			}
		}
	}
	// Sort found generations starting from the newest.
	sort.Slice(generations, func(i, j int) bool { return generations[i] > generations[j] })
	return generations, nil
}

// recordHistory backups a file. If the file doesn't exist or the current
// version is already saved, it does nothing. The file name is normalized.
func (S *Store) recordHistory(file string) error {
	file = normalizeName(file, false)
	path := S.filePath(file, false)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil // File doesn't exist.
	}
	generations, err := S.History(file)
	if err != nil {
		return fmt.Errorf("recordHistory %s: %w", file, err)
	}
	if len(generations) != 0 {
		latest := S.filePath(file, true) + "@" + strconv.FormatUint(generations[0], 10)
		if eq, err := compareFiles(path, latest); err != nil {
			return fmt.Errorf("recordHistory %s: %w", file, err)
		} else if eq {
			return nil // This version is already saved
		}
	}
	// Capturing
	if err := copyFile(path, S.filePath(file, true)+"@"+strconv.FormatUint(S.GetGeneration(true), 10), false); err != nil {
		return fmt.Errorf("recordHistory %s: %w", file, err)
	}
	return nil
}

// compareFiles return true if both files are equal.
// Based on https://stackoverflow.com/a/30038571
func compareFiles(file1, file2 string) (bool, error) {
	f1s, err := os.Stat(file1)
	if err != nil {
		return false, fmt.Errorf("compareFiles %s %s: %w", file1, file2, err)
	}
	f2s, err := os.Stat(file2)
	if err != nil {
		return false, fmt.Errorf("compareFiles %s %s: %w", file1, file2, err)
	}
	if f1s.Size() != f2s.Size() {
		return false, nil
	}

	f1, err := os.Open(file1)
	if err != nil {
		return false, fmt.Errorf("compareFiles %s %s: %w", file1, file2, err)
	}
	f2, err := os.Open(file2)
	if err != nil {
		return false, fmt.Errorf("compareFiles %s %s: %w", file1, file2, err)
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
				return false, fmt.Errorf("compareFiles %s %s: %w", file1, file2, err1)
			} else {
				return false, fmt.Errorf("compareFiles %s %s: %w", file1, file2, err2)
			}
		}
		if !bytes.Equal(b1, b2) {
			return false, nil
		}
	}
}

// copyFile is a helper function to copy files. If overwrite flag is set
// to false and the target file exists, the file will not be copied
// and an error will be returned.
func copyFile(from, to string, overwrite bool) error {
	f1, err := os.Open(from)
	if err != nil {
		return fmt.Errorf("copyFile %s %s: %w", from, to, err)
	}
	defer f1.Close()
	if err = os.MkdirAll(filepath.Dir(to), 0755); err != nil {
		return fmt.Errorf("copyFile %s %s: %w", from, to, err)
	}
	flags := 0
	if overwrite {
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	} else {
		flags = os.O_CREATE | os.O_WRONLY | os.O_EXCL
	}
	f2, err := os.OpenFile(to, flags, 0644)
	if err != nil {
		return fmt.Errorf("copyFile %s %s: %w", from, to, err)
	}
	defer f2.Close()
	if _, err = io.Copy(f2, f1); err != nil {
		return fmt.Errorf("copyFile %s %s: %w", from, to, err)
	}
	return nil
}

// Overwrite returns a file descriptor for writing.
// If the file exists, it is truncated.
func (S *Store) Overwrite(file string) (*os.File, error) {
	if err := S.recordHistory(file); err != nil {
		return nil, fmt.Errorf("overwrite %s: %w", file, err)
	}
	f, err := os.OpenFile(S.filePath(file, false), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return f, fmt.Errorf("overwrite %s: %w", file, err)
	} else {
		return f, nil
	}
}

// Open opens given file for reading. If generation is non-zero, it opens a historic version.
func (S *Store) Open(file string, generation uint64) (*os.File, error) {
	if generation == 0 {
		f, err := os.Open(S.filePath(file, false))
		if err != nil {
			return f, fmt.Errorf("open %s: %w", file, err)
		} else {
			return f, nil
		}
	} else {
		f, err := os.Open(S.filePath(file, true) + "@" + strconv.FormatUint(generation, 10))
		if err != nil {
			return f, fmt.Errorf("open %s: %w", file, err)
		} else {
			return f, nil
		}
	}
}

// Copy copies a file.
func (S *Store) Copy(from, to string) error {
	if err := S.recordHistory(to); err != nil {
		return fmt.Errorf("copy %s %s: %w", from, to, err)
	}
	if err := copyFile(S.filePath(from, false), S.filePath(to, false), true); err != nil {
		return fmt.Errorf("copy %s %s: %w", from, to, err)
	}
	return nil
}

// Move moves a file.
func (S *Store) Move(from, to string) error {
	if err := S.recordHistory(to); err != nil {
		return fmt.Errorf("move %s %s: %w", from, to, err)
	}
	if err := S.recordHistory(from); err != nil {
		return fmt.Errorf("move %s %s: %w", from, to, err)
	}
	if err := os.Rename(S.filePath(from, false), S.filePath(to, false)); err != nil {
		return fmt.Errorf("move %s %s: %w", from, to, err)
	}
	return nil
}

// Remove removes a file.
func (S *Store) Remove(file string) error {
	if err := S.recordHistory(file); err != nil {
		return fmt.Errorf("remove %s: %w", file, err)
	}
	if err := os.Remove(S.filePath(file, false)); err != nil {
		return fmt.Errorf("remove %s: %w", file, err)
	}
	return nil
}

// Stat runs os.Stat on the specified file.
func (S *Store) Stat(file string, history bool) (fs.FileInfo, error) {
	return os.Stat(S.filePath(file, history))
}

// List lists all files. If history is true, returns all backed up files'
// names, without the version string.
func (S *Store) List(history bool) ([]string, error) {
	files := []string{}
	dir, err := os.ReadDir(S.filePath("", history))
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	processed := make(map[string]bool)
	for _, entry := range dir {
		if entry.Name() == ".history" {
			continue
		}
		file := baseName(entry.Name())
		if !processed[file] {
			files = append(files, file)
			processed[file] = true
		}
	}
	return files, nil
}
