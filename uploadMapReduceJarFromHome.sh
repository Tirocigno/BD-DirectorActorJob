#!/usr/bin/env bash

./gradlew mapReduceJar

scp -P 2201 ./build/libs/*-mr.jar fnaldini@localhost:/home/fnaldini/exam/