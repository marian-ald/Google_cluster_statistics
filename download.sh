#!/bin/bash


for (( i = 0; i < 500; i++ )); do
	replace=""
	if [[ ${i} -lt 10 ]]; then
		replace="000${i}"
	elif [[ ${i} -gt 9 && ${i} -lt 100 ]]; then 
		replace="00${i}"
	elif [[ ${i} -gt 99 && ${i} -lt 500 ]]; then 
		replace="0${i}"
	fi

	url="gs://clusterdata-2011-2/task_events/part-0${replace}-of-00500.csv.gz"
	gsutil cp $url data/task_events
	# echo $url

	echo "Downloaded file ${i}"
done