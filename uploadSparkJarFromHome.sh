#!/usr/bin/env bash

./gradlew mapReduceJar

scp -P 2201 ./build/libs/*-spark.jar fnaldini@localhost:/home/fnaldini/project/