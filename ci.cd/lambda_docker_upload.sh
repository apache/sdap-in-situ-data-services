#!/bin/bash


aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 848373852523.dkr.ecr.us-west-2.amazonaws.com
docker tag ${DOCKER_IMAGE}:${DOCKER_TAG} 848373852523.dkr.ecr.us-west-2.amazonaws.com/cdms:${DOCKER_TAG}
docker push 848373852523.dkr.ecr.us-west-2.amazonaws.com/cdms:${DOCKER_TAG}