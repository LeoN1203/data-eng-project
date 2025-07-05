#!/bin/bash

# =============================================================================
# START SERVICES SCRIPT
# =============================================================================
# This script starts all the infrastructure services needed for the data pipeline:
# - Zookeeper
# - Kafka
# - Spark Master
# - Spark Worker
# - PostgreSQL (for Grafana)
# =============================================================================

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}🚀 Starting Data Pipeline Infrastructure Services...${NC}"

# Start all services
echo -e "${YELLOW}Starting services...${NC}"
docker compose -f docker/docker-compose.yml up -d

# Wait a bit for services to start
sleep 10

# Check status
echo -e "${YELLOW}Checking service status...${NC}"
docker compose -f docker/docker-compose.yml ps

# Show service information
echo ""
echo -e "${GREEN}✅ Infrastructure services started successfully!${NC}"
echo ""
echo -e "${BLUE}📊 Service Access Points:${NC}"
echo "🎯 Spark Master UI:     http://localhost:8080"
echo "🎯 Spark Worker UI:     http://localhost:8081"
echo "📊 Kafka:               localhost:9092"
echo "🗄️  PostgreSQL:          localhost:5432"
echo "   - Database: grafana_db"
echo "   - User: grafana"
echo "   - Password: grafana"
echo ""
echo -e "${BLUE}🔧 Useful Commands:${NC}"
echo "📊 Check status:        docker compose -f docker/docker-compose.yml ps"
echo "📝 View logs:           docker compose -f docker/docker-compose.yml logs -f [service-name]"
echo "🛑 Stop services:       docker compose -f docker/docker-compose.yml down"
echo "🗑️  Clean volumes:       docker compose -f docker/docker-compose.yml down -v"
echo "🔄 Restart service:     docker compose -f docker/docker-compose.yml restart [service-name]"
echo ""
echo -e "${GREEN}🎉 Ready to run data pipeline jobs!${NC}" 