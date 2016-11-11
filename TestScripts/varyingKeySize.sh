#!/bin/bash
letterlist=(Y)
bytelist=(10 20 40 80 160 320 640)
keylist=(key1 key2 key3 key4 key5 key6 key7 key8 key9 key10)


for byte in ${bytelist[@]};
do
	for letter in ${letterlist[@]}; 
	do 
		for key in ${keylist[@]};   
		do
			echo Putting value $letter of $byte bytes for key: $key
			python testClient.py 1 PUT $key $letter $byte >> keysizeputtest$byte.txt
			sleep 3s
		done
		wait ${!}
	done
	sleep 1m
done


for byte in ${bytelist[@]};
do
	for letter in ${letterlist[@]}; 
	do 
		for key in ${keylist[@]};   
		do
			echo Getting value $letter of $byte bytes for key: $key
			python testClient.py 1 GET $key $letter $byte >> keysizegettest$byte.txt
			sleep 3s
		done
		wait ${!}
	done
done
