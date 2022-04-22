#!/bin/bash

declare -a producers=(
    1
    2
    4
)
declare -a consumers=(
    4
    8
    12
    16
)

for i in "${producers[@]}"
do
    for j in "${consumers[@]}"
    do
        echo "Producers: $i, Consumers: $j"
        time python3 solution.py --num_producers=$i --num_consumers=$j --name="producers_$i consumers_$j"
    done
done