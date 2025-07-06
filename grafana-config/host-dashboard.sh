#!/bin/bash

docker compose up -d

ngrok http 3001 &
