#!/bin/bash
for i in {1..10}; do
    for j in {1..20}; do
    # k=$i*$j
    # echo -e "\nROUND $k\n"

    python client.py 0 &
    python client2.py 0 &
  done
  wait
done 2>/dev/null