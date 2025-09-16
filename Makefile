# Makefile for managing Docker Compose services

# Start services in detached mode
up:
	docker compose up -d


# Stop and remove services, networks, and volumes defined in docker-compose.yml
down:
	docker compose down --volumes --remove-orphans

# Stop all services without removing them
stop:
	docker compose stop

# Start all stopped services
start:
	docker compose start
	
# Restart all services
restart:
	docker compose restart
	
# Build or rebuild images
build:
	docker compose build --no-cache

bake:
	docker buildx bake --no-cache

# Build or rebuild images
bcache:
	docker compose build

# Prune unused containers, images, networks, and volumes
prune:
	docker system prune -a --volumes -f

# Delete all stopped containers
clean-containers:
	docker container prune -f

# Delete all unused images
clean-images:
	docker image prune -a -f

# Delete all unused volumes
clean-volumes:
	docker volume prune -f

# Delete all unused networks
clean-networks:
	docker network prune -f

# Comprehensive cleanup: stop and remove all services, containers, images, volumes, and networks
clean-all: down clean-containers clean-images clean-volumes clean-networks

# View logs for all services
logs:
	docker compose logs -f

# View logs for a specific service (e.g., make logs-service SERVICE=spark-master)
logs-service:
	docker compose logs -f $(SERVICE)

# Check the status of all servicesmake 
ps:
	docker compose ps


# Declare all targets as phony to avoid conflicts with files
.PHONY: up down stop start restart build prune clean-containers clean-images clean-volumes c