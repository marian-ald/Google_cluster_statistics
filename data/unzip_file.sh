#!/bin/bash



for file in * ; do
        gzip -d $file
        echo "$file"
done
