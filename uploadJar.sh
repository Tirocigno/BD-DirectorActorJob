#!/usr/bin/env bash

./gradlew shadowJar

scp ./build/libs/*.jar fnaldini@isi-vclust7.csr.unibo.it:/home/fnaldini/project/