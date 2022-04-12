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
func getGenerationFromFileName(path string) (generation uint64) {
	if path == "" {
		return 0
	}
	path = strings.TrimRight(path, "/")
	for i := len(path) - 2; i >= 0; i-- {
		switch path[i] {
		case '/', '\\':
			return 0
		case '@':
			generation, _ = strconv.ParseUint(path[i+1:], 10, 64)
			return
		}
	}
	return
}

// cleanDirectory removes an empty directory and recursively its empty parents.
func cleanDirectory(path string) error {
	if s, err := os.Stat(path); err != nil || !s.IsDir() {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	_, err = f.Readdirnames(1)
	f.Close()
	if err == io.EOF { // Directory is empty
		os.Remove(path)
		return cleanDirectory(filepath.Dir(path))
	} else if err != nil {
		return err
	}
	return nil
}

// cleanDirectoryStructure removes all empty directories in
// the tree rooted at path. The root folder is not deleted.
func cleanDirectoryStructure(path string) error {
	unneeded := make(map[string]bool)
	var mark func(path string) error
	mark = func(path string) error {
		if s, err := os.Stat(path); err != nil || !s.IsDir() {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		entries, err := f.ReadDir(0)
		f.Close()
		if err != nil {
			return err
		}
		empty := true
		for _, entry := range entries {
			if entry.IsDir() {
				err := mark(filepath.Join(path, entry.Name()))
				if err != nil {
					return err
				}
				if !unneeded[filepath.Join(path, entry.Name())] {
					empty = false
				}
			} else {
				empty = false
			}
		}
		if empty {
			unneeded[path] = true
		}
		return nil
	}
	err := mark(path)
	if err != nil {
		return err
	}
	delete(unneeded, path)
	toBeDeleted := []string{}
	for f := range unneeded {
		toBeDeleted = append(toBeDeleted, f)
	}
	sort.Slice(toBeDeleted, func(i, j int) bool {
		return strings.Count(toBeDeleted[i], string(filepath.Separator)) > strings.Count(toBeDeleted[j], string(filepath.Separator))
	})
	for _, f := range toBeDeleted {
		os.Remove(f)
	}
	return nil
}

// fixIllegalFilenames renames all files whose names are illegal.
// It skips `path/.history`.
func fixIllegalFilenames(path string) error {
	var fixChildrenOf func(path string) error
	fixChildrenOf = func(currentPath string) error {
		if s, err := os.Stat(currentPath); err != nil || !s.IsDir() {
			return err
		}
		f, err := os.Open(currentPath)
		if err != nil {
			return err
		}
		entries, err := f.ReadDir(0)
		f.Close()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if path == currentPath && entry.Name() == ".history" {
				continue
			}
			if entry.IsDir() {
				if err := fixChildrenOf(filepath.Join(currentPath, entry.Name())); err != nil {
					return err
				}
			}
			if checkPath(entry.Name()) != nil {
				os.Rename(filepath.Join(currentPath, entry.Name()), filepath.Join(currentPath, fixPath(entry.Name())))
			}
		}
		return nil
	}
	return fixChildrenOf(path)
}

// New opens or creates a new store.
func New(root string) (S Store, err error) {
	S = Store{Root: root, Generation: 1}
	if err = os.MkdirAll(root, 0755); err != nil {
		return
	}
	if err = cleanDirectoryStructure(root); err != nil {
		return
	}
	if err = fixIllegalFilenames(root); err != nil {
		return
	}
	if err = os.MkdirAll(root+"/.history", 0755); err != nil {
		return
	}
	filepath.WalkDir(root+"/.history", fs.WalkDirFunc(func(path string, _ fs.DirEntry, _ error) error {
		if g := getGenerationFromFileName(path); g > S.Generation {
			S.Generation = g
		}
		return nil
	}))
	return
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
	pathElements := strings.Split(filepath.Clean(path), string(filepath.Separator))
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

func fixPath(path string) (fixed string) {
	if strings.HasPrefix(path, "/") {
		fixed = "/"
	}
	pathElements := strings.Split(filepath.Clean(path), string(filepath.Separator))
	for _, element := range pathElements {
		fixed = filepath.Join(fixed, strings.Replace(strings.TrimPrefix(element, "."), "@", "_", -1))
	}
	return
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
func (S *Store) FileHistory(path string) (history []uint64, err error) {
	dir, err := os.ReadDir(filepath.Dir(S.realPath(path, true)))
	if err != nil {
		return
	}
	for _, entry := range dir {
		if n := entry.Name(); strings.HasPrefix(n, filepath.Base(path)+"@") {
			if g := getGenerationFromFileName(n); g != 0 {
				history = append(history, g)
			}
		}
	}
	// Sort found generations starting from the newest.
	sort.Slice(history, func(i, j int) bool { return history[i] > history[j] })
	return
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
	if err = os.MkdirAll(filepath.Dir(to), 0755); err != nil {
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
	if _, err = io.Copy(f2, f1); err != nil {
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
		if eq, err := compareFiles(currentPath, latestPath); err != nil {
			return err
		} else if eq {
			return nil // This version is already saved
		}
	}
	// Capturing
	copy(currentPath, S.realPath(path, true)+"@"+strconv.FormatUint(S.GetGeneration(true), 10), false)
	return nil
}

// Write returns a file descriptor for writing.
// If the file exists, it is truncated.
func (S *Store) Write(path string) (*os.File, error) {
	if err := S.captureHistory(path); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(S.realPath(path, false)), 0755); err != nil {
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
	if err := S.captureHistory(to); err != nil {
		return err
	}
	return copy(S.realPath(from, false), S.realPath(to, false), true)
}

// Move moves a file.
func (S *Store) Move(from, to string) error {
	if err := S.captureHistory(to); err != nil {
		return err
	}
	if err := S.captureHistory(from); err != nil {
		return err
	}
	if err := os.Rename(S.realPath(from, false), S.realPath(to, false)); err != nil {
		return err
	}
	return cleanDirectory(filepath.Dir(S.realPath(to, false)))
}

// Remove removes a file.
func (S *Store) Remove(path string) error {
	if err := S.captureHistory(path); err != nil {
		return err
	}
	if err := os.Remove(S.realPath(path, false)); err != nil {
		return err
	}
	return cleanDirectory(filepath.Dir(S.realPath(path, false)))
}

// List lists files (and not directories) in the specified directory.
// List returns paths relative to root, not to the path specified as the argument.
func (S *Store) List(path string, history bool, recursive bool) (listing []string, err error) {
	entries, err := os.ReadDir(S.realPath(path, history))
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.Name() == ".history" {
			continue
		}
		entryPath := filepath.Join(path, entry.Name())
		if !entry.IsDir() {
			listing = append(listing, entryPath)
		}
		if recursive && entry.IsDir() {
			var children []string
			children, err = S.List(entryPath, history, true)
			if err != nil {
				return
			}
			listing = append(listing, children...)
		}
	}
	return
}

// Stat runs os.Stat on the specified file.
func (S *Store) Stat(path string, history bool) (fs.FileInfo, error) {
	if err := checkPath(path); err != nil {
		return nil, err
	}
	fi, err := os.Stat(S.realPath(path, history))
	if err != nil {
		return nil, err
	}
	return fi, nil
}
