package config

import (
	"fmt"
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

// VPS represents a single VPS server configuration.
type VPS struct {
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password,omitempty"`
	KeyPath  string `yaml:"key_path,omitempty"`
}

// SavedCommand represents a saved command snippet.
type SavedCommand struct {
	Name    string `yaml:"name"`
	Command string `yaml:"command"`
}

// Config holds the entire application configuration.
type Config struct {
	BotToken        string         `yaml:"bot_token"`
	AuthorizedUsers []int64        `yaml:"authorized_users"`
	Servers         []VPS          `yaml:"servers"`
	MaxOutput       int            `yaml:"max_output"`
	SavedCommands   []SavedCommand `yaml:"saved_commands,omitempty"`

	Mu       sync.RWMutex `yaml:"-"`
	filePath string
}

// Load reads and parses the YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{
		MaxOutput: 4000,
		filePath:  path,
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.BotToken == "" {
		return nil, fmt.Errorf("bot_token is required in config")
	}
	if len(cfg.AuthorizedUsers) == 0 {
		return nil, fmt.Errorf("at least one authorized_user is required")
	}

	for i := range cfg.Servers {
		if cfg.Servers[i].Port == 0 {
			cfg.Servers[i].Port = 22
		}
	}

	return cfg, nil
}

// Save writes the current config state back to disk.
func (c *Config) Save() error {
	c.Mu.RLock()
	defer c.Mu.RUnlock()
	return c.saveLocked()
}

// saveLocked writes config to disk. Caller must hold at least Mu.RLock.
func (c *Config) saveLocked() error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(c.filePath, data, 0600)
}

// AddServer adds a VPS to the config and saves.
func (c *Config) AddServer(v VPS) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if v.Port == 0 {
		v.Port = 22
	}
	c.Servers = append(c.Servers, v)
	return c.saveLocked()
}

// RemoveServer removes a VPS by name and saves.
func (c *Config) RemoveServer(name string) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	for i, s := range c.Servers {
		if s.Name == name {
			c.Servers = append(c.Servers[:i], c.Servers[i+1:]...)
			return c.saveLocked()
		}
	}
	return fmt.Errorf("server %q not found", name)
}

// GetServer returns a VPS by name.
func (c *Config) GetServer(name string) (VPS, bool) {
	c.Mu.RLock()
	defer c.Mu.RUnlock()
	for _, s := range c.Servers {
		if s.Name == name {
			return s, true
		}
	}
	return VPS{}, false
}

// UpdateServer updates a server by its current name. If newVPS.Name differs, the server is renamed.
func (c *Config) UpdateServer(currentName string, newVPS VPS) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	for i, s := range c.Servers {
		if s.Name == currentName {
			if newVPS.Port == 0 {
				newVPS.Port = 22
			}
			c.Servers[i] = newVPS
			return c.saveLocked()
		}
	}
	return fmt.Errorf("server %q not found", currentName)
}

// IsAuthorized checks if a user ID is in the authorized list.
func (c *Config) IsAuthorized(userID int64) bool {
	c.Mu.RLock()
	defer c.Mu.RUnlock()
	for _, id := range c.AuthorizedUsers {
		if id == userID {
			return true
		}
	}
	return false
}

// AddSavedCommand adds or updates a saved command snippet.
func (c *Config) AddSavedCommand(name, cmd string) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	for i, sc := range c.SavedCommands {
		if sc.Name == name {
			c.SavedCommands[i].Command = cmd
			return c.saveLocked()
		}
	}
	c.SavedCommands = append(c.SavedCommands, SavedCommand{Name: name, Command: cmd})
	return c.saveLocked()
}

// RemoveSavedCommand removes a saved command by name.
func (c *Config) RemoveSavedCommand(name string) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	for i, sc := range c.SavedCommands {
		if sc.Name == name {
			c.SavedCommands = append(c.SavedCommands[:i], c.SavedCommands[i+1:]...)
			return c.saveLocked()
		}
	}
	return fmt.Errorf("snippet %q not found", name)
}

// GetSavedCommands returns a copy of all saved commands.
func (c *Config) GetSavedCommands() []SavedCommand {
	c.Mu.RLock()
	defer c.Mu.RUnlock()
	result := make([]SavedCommand, len(c.SavedCommands))
	copy(result, c.SavedCommands)
	return result
}

// GetSavedCommand returns a saved command by name.
func (c *Config) GetSavedCommand(name string) (string, bool) {
	c.Mu.RLock()
	defer c.Mu.RUnlock()
	for _, sc := range c.SavedCommands {
		if sc.Name == name {
			return sc.Command, true
		}
	}
	return "", false
}
