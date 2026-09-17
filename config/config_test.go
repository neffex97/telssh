package config

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestConfigAtomicSave(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telssh-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.yaml")
	cfg := &Config{
		filePath:        configPath,
		BotToken:        "test-token",
		AuthorizedUsers: []int64{12345},
		MaxOutput:       4000,
		Servers: []VPS{
			{Name: "srv1", Host: "1.2.3.4", Port: 22, User: "root"},
		},
	}

	if err := cfg.Save(); err != nil {
		t.Fatalf("cfg.Save failed: %v", err)
	}

	loaded, err := Load(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if loaded.BotToken != "test-token" || len(loaded.Servers) != 1 {
		t.Fatalf("unexpected loaded config: %+v", loaded)
	}

	// Test concurrent updates to ensure Mu.Lock in Save() prevents race conditions
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = cfg.AddServer(VPS{
				Name: filepath.Join("server", string(rune('a'+idx))),
				Host: "127.0.0.1",
				Port: 22,
				User: "test",
			})
		}(i)
	}
	wg.Wait()
}
