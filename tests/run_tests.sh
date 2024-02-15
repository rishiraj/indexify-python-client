#!/bin/bash

docker-compose up -d

serverReady=false
while [ "$serverReady" != true ]; do
    output=$(curl --silent --fail http://localhost:8900/extractors | jq '.extractors | length' 2>/dev/null)
    if [[ $? -eq 0 && "$output" -eq 2 ]]; then
        echo "Server is ready with 2 extractors."
        serverReady=true
    else
        printf 'Waiting for server to start with 2 extractors...\n'
        sleep 5
    fi
done



# curl tests

# create namespace
curl -v -X POST http://localhost:8900/namespaces -H "Content-Type: application/json" -d '
{
	"name": "test.search",
	"extractor_bindings":[],
	"labels":{}
}'

# add text
curl -v -X POST http://localhost:8900/namespaces/test.search/add_texts \
-H "Content-Type: application/json" \
-d '{"documents": [
        {"text": "This is a test", "labels":{"source":"test"}}
    ]}'

# bind extractor
curl -v -X POST http://localhost:8900/namespaces/test.search/extractor_bindings \
-H "Content-Type: application/json" \
-d '{"extractor": "tensorlake/minilm-l6", "name": "minilml6", "input_params":{}, "filters_eq": "source:test" }'

# search
sleep 5
curl -v -X POST http://localhost:8900/namespaces/test.search/search \
-H "Content-Type: application/json" \
-d '{ 
	"index": "minilml6.embedding", 
	"query": "test", 
	"k": 3
}'

docker-compose down




# pytest integration_test.py::TestIntegrationTest
# pytest_exit_status=$?

# docker-compose down

# exit $pytest_exit_status
