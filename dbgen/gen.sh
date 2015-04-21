#! /bin/sh

for ((  i = 1 ;  i <= 50;  i++  ))
do
  echo "Building $i-th step of data"
  ./dbgen -s 1000 -S $i -C 50 -T c -v > gen_log.dat
done
echo "Dataset generated!"
