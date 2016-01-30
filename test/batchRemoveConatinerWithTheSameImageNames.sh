#!/bin/bash
docker rm -f $(docker ps | awk '{print $2,$1}' |grep "^yan/ccnlp:2.2"|awk '{print $2}'D)