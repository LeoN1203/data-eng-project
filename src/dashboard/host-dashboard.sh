#!/bin/bash

docker compose up -d

# Run ngrok in the background to avoid blocking the script
ngrok http 3001 &
