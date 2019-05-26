#!/bin/bash

for i in {1..100}
do
   curl localhost:8080/v1/jobs -d "{
    \"name\": \"test_job_$i\",
    \"schedule\": \"@every 10s\",
	\"executor\": \"shell\",
  	\"executor_config\": {
    	\"command\": \"echo $1\"
  	}
}"
done
