package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"telssh/bot"
	"telssh/config"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Loaded %d server(s), %d authorized user(s)", len(cfg.Servers), len(cfg.AuthorizedUsers))

	b, err := bot.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create bot: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go b.Start()

	<-sigCh
	log.Println("Shutting down...")
	b.Stop()
	log.Println("Bot stopped.")
}
