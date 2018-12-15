#!/usr/bin/env bash

rm -rf $HOME/.m2/*
rm -rf target
mvn package -DskipTests
docker-compose build --force-rm
