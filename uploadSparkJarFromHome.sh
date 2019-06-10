#!/usr/bin/env bash

./gradlew sparkJar

scp -P 2201 ./build/libs/*-spark.jar fnaldini@localhost:/home/fnaldini/exam/