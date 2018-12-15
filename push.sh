#!/usr/bin/env bash

# one time password request / login
docker login -u jameshnsears

push_to_docker_hub xqa-ingest-balancer

docker search jameshnsears

exit $?
