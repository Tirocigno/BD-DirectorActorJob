#!/usr/bin/env bash

./gradlew sparkJar

scp ./build/libs/*-spark.jar fnaldini@isi-vclust7.csr.unibo.it:/home/fnaldini/exam/