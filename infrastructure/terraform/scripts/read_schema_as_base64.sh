#!/bin/bash

export BASE_SCHEMA=$(jq '.schema_fields' -r < $1 | base64)
# cat $1 | jq --arg variable "$BASE_SCHEMA" '.schema_fields = $variable' | jq '.schema_fields'
jq -n --arg variable "$BASE_SCHEMA" '{"schema_fields":$variable}'