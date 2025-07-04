#!/bin/bash

# Data Pipeline Docker Services Startup Script
echo "🚀 Starting Data Pipeline Services..."

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  Creating .env file from .env.example..."
    cp .env.example .env
    echo "📝 Please edit .env file with your AWS credentials before continuing!"
    echo "   Required variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION"
    echo ""
    read -p "Press Enter after updating .env file, or Ctrl+C to exit..."
fi

# Start services
echo "🐳 Starting Docker Compose services..."
docker-compose -f docker/docker-compose.yml up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to start..."
sleep 30

# Check service status
echo ""
echo "📊 Service Status:"
docker-compose -f docker/docker-compose.yml ps

echo ""
echo "🎯 Access Points:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📈 Spark Master UI:     http://localhost:8080"
echo "📜 Spark History:       http://localhost:18080"
echo "🔄 Kafka UI:            http://localhost:8090"
echo "📓 Jupyter Lab:         http://localhost:8888"
echo ""
echo "🔌 Connection Details:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎭 Spark Master:        spark://localhost:7077"
echo "📨 Kafka Bootstrap:     localhost:9092"
echo "🏗️  Zookeeper:          localhost:2181"
echo ""
echo "💡 Useful Commands:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📝 View logs:           docker-compose -f docker/docker-compose.yml logs -f [service-name]"
echo "🛑 Stop services:       docker-compose -f docker/docker-compose.yml down"
echo "🗑️  Clean volumes:       docker-compose -f docker/docker-compose.yml down -v"
echo "🔄 Restart service:     docker-compose -f docker/docker-compose.yml restart [service-name]"
echo ""
echo "✅ All services are starting up! Check the URLs above in a few moments." 