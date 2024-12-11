package main

import (
	_ "ChubbyGo/Config"
	"ChubbyGo/Connect"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "Config/server_config.json", "path to server configuration file")
	flag.Parse()

	// Validate config file exists
	if _, err := os.Stat(*configPath); os.IsNotExist(err) {
		log.Fatalf("Config file not found: %s", *configPath)
	}

	// Create and start server
	cfg := Connect.CreatServer(3, *configPath)
	if err := cfg.StartServer(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down server...")
}
