#!/bin/bash
#Now this removes all the dangling non intermediate <none> images:

docker rmi `docker images --filter 'dangling=true' -q --no-trunc`
