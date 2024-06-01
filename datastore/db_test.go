package datastore

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 45)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"1", "v1"},
		{"2", "v2"},
		{"3", "v3"},
	}

	outFile, err := os.Open(filepath.Join(dir, outFileName+"0"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			key := pair[0]
			value := pair[1]

			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s=%s: %s", key, value, err)
			}
			actual, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if actual != value {
				t.Errorf("Bad value returned expected %s, got %s", value, actual)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			key := pair[0]
			value := pair[1]

			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s=%s: %s", key, value, err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 45)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			key := pair[0]
			value := pair[1]

			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s=%s: %s", key, value, err)
			}
			actual, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if actual != value {
				t.Errorf("Bad value returned expected %s, got %s", value, actual)
			}
		}
	})
}

func TestDb_Segmentation(t *testing.T) {
	t.Run("check creation of new file", func(t *testing.T) {
		db, saveDirectory := prepareDb(t)
		defer db.Close()
		defer os.RemoveAll(saveDirectory)

		put(t, db, "1", "v1")
		put(t, db, "2", "v2")
		put(t, db, "3", "v3")
		put(t, db, "2", "v4")

		if len(db.segments) != 2 {
			t.Errorf("An error occurred during segmentation. Expected 2 files, but received %d.", len(db.segments))
		}
	})

	t.Run("check starting segmentation", func(t *testing.T) {
		db, saveDirectory := prepareDb(t)
		defer db.Close()
		defer os.RemoveAll(saveDirectory)

		put(t, db, "1", "v1")
		put(t, db, "2", "v2")
		put(t, db, "3", "v3")
		put(t, db, "2", "v4")
		put(t, db, "2", "v5")
		put(t, db, "4", "v6")
		put(t, db, "4", "v7")

		if len(db.segments) != 3 {
			t.Errorf("An error occurred during segmentation. Expected 3 files, but received %d.", len(db.segments))
		}

		time.Sleep(4 * time.Second)

		if len(db.segments) != 2 {
			t.Errorf("An error occurred during segmentation. Expected 2 files, but received %d.", len(db.segments))
		}
	})

	t.Run("check not storing new values of duplicate keys after segmentation", func(t *testing.T) {
		db, saveDirectory := prepareDb(t)
		defer db.Close()
		defer os.RemoveAll(saveDirectory)

		put(t, db, "1", "v1")
		put(t, db, "2", "v2")
		put(t, db, "3", "v3")
		put(t, db, "2", "v4")
		put(t, db, "2", "v5")
		put(t, db, "2", "v6")
		put(t, db, "2", "v7")

		if len(db.segments) != 3 {
			t.Errorf("An error occurred during segmentation. Expected 3 files, but received %d.", len(db.segments))
		}

		time.Sleep(4 * time.Second)

		if len(db.segments) != 2 {
			t.Errorf("An error occurred during segmentation. Expected 2 files, but received %d.", len(db.segments))
		}

		actual, err := db.Get("2")
		if err != nil {
			t.Fatal(err)
		}
		if actual != "v7" {
			t.Errorf("An error occurred during segmentation. Expected value: %s, Actual one: %s", "v7", actual)
		}
	})

	t.Run("check size", func(t *testing.T) {
		db, saveDirectory := prepareDb(t)
		defer db.Close()
		defer os.RemoveAll(saveDirectory)

		put(t, db, "1", "v1")
		put(t, db, "2", "v2")
		put(t, db, "3", "v3")
		put(t, db, "2", "v4")
		put(t, db, "2", "v5")
		put(t, db, "2", "v6")
		put(t, db, "2", "v7")

		if len(db.segments) != 3 {
			t.Errorf("An error occurred during segmentation. Expected 3 files, but received %d.", len(db.segments))
		}

		time.Sleep(4 * time.Second)

		if len(db.segments) != 2 {
			t.Errorf("An error occurred during segmentation. Expected 2 files, but received %d.", len(db.segments))
		}

		file, err := os.Open(db.segments[0].filePath)
		defer file.Close()

		if err != nil {
			t.Error(err)
		}
		inf, _ := file.Stat()
		if inf.Size() != int64(165) {
			t.Errorf("An error occurred during segmentation. Expected size %d, Actual one: %d", 165, inf.Size())
		}
	})
}

func prepareDb(t *testing.T) (db *Db, saveDirectory string) {
	saveDirectory, err := ioutil.TempDir("", "testDir")
	if err != nil {
		t.Fatal(err)
	}

	db, err = NewDb(saveDirectory, 165)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func put(t *testing.T, db *Db, key, value string) {
	err := db.Put(key, value)
	if err != nil {
		t.Errorf("An error occurred during segmentation. %s", err)
	}
}

func TestDb_HashCheck(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 150)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Put("key1", "value1")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put("key2", "value2")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put("key3", "value3")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("hash check on Get operation", func(t *testing.T) {
		val, err := db.Get("key1")
		if err != nil {
			t.Errorf("Failed to get existing key: %v", err)
		}
		if val != "value1" {
			t.Errorf("Bad value returned expected value1, got %s", val)
		}

		// simulate corrupted data by directly modifying the file
		filePath := db.segments[0].filePath
		file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()

		pos := db.segments[0].index["key1"]
		file.Seek(pos+12+int64(len("key1")), 0)
		file.Write([]byte("corupt"))

		// try to get the corrupted key
		_, err = db.Get("key1")
		if !errors.Is(err, ErrHashMismatch) {
			t.Errorf("Expected ErrHashMismatch for corrupted data, got: %v", err)
		}

		// confirm other key still present and uncorrupted
		val, err = db.Get("key2")
		if err != nil {
			t.Errorf("Failed to get existing key: %v", err)
		}
		if val != "value2" {
			t.Errorf("Bad value returned expected value2, got %s", val)
		}
	})
}
