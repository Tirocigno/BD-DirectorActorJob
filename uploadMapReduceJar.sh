#!/usr/bin/env bash

./gradlew mapReduceJar

scp ./build/libs/*-mr.jar fnaldini@isi-vclust7.csr.unibo.it:/home/fnaldini/project/