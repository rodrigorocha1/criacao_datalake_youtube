#!/bin/bash
docker stop $(docker ps -aq) # Para todos os containers
docker rm $(docker ps -aq)   #
docker volume rm $(docker volume ls -q)
docker image rm $(docker image ls -q)
docker network rm $(docker network ls -q)