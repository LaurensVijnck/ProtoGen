export GOOGLE_APPLICATION_CREDENTIALS=/Users/lvijnck/Documents/google-cloud-sdk/geometric-ocean-284614-77fba73ca7b0.json

# Activate venv
VENV_PATH=/Users/lvijnck/Desktop/env/ProtoToBQ/bin/activate
source $VENV_PATH

# Add path to executable to path variable
# FUTURE: Create venv as part of the plugin
SRC_DIR=/Users/lvijnck/Desktop/ProtoBQGeneration/ProtoToBQ/
export PATH=$SRC_DIR:$PATH

DST_DIR=target/generated-sources/protobuf

rm -rf $DST_DIR
mkdir -p $DST_DIR/java
mkdir -p $DST_DIR/bigquery

# Compile protos
protoc -I=. \
    --java_out=$DST_DIR/java \
    --experimental_allow_proto3_optional \
    $1

# Invoke Custom BigQuery plugin
protoc $1 \
  --experimental_allow_proto3_optional \
  --bq_out=$DST_DIR/bigquery