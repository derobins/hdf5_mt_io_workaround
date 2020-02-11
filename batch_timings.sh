#!/bin/bash

for n_threads in 1 2 4 8 16 32 64
do
    for i in {1..5}
    do
        ./reader -a posixmt -n $n_threads data.h5
    done
done

