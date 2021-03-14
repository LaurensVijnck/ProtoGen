# FUTURE: Remove when proto-to-bq no longer requires this!
export GOOGLE_APPLICATION_CREDENTIALS=/Users/lvijnck/Documents/google-cloud-sdk/geometric-ocean-284614-77fba73ca7b0.json

# Activate venv
# FUTURE: Create venv as part of the plugin
VENV_PATH=/Users/lvijnck/Desktop/env/ProtoToBQ/bin/activate
source $VENV_PATH

# Add path to executable Proto-to-bq plugin to path variable
# Proto will automatically detect the plugin due to the file format
export PATH=$1:$PATH

DST_DIR=target/generated-sources/protobuf

rm -rf $DST_DIR
mkdir -p $DST_DIR/java
mkdir -p $DST_DIR/bigquery

# Compile protos
protoc -I=$1 \
    --java_out=$DST_DIR/java \
    --experimental_allow_proto3_optional \
    $1/$2

### Invoke Custom BigQuery plugin
protoc -I=$1 \
  --experimental_allow_proto3_optional \
  --bq_out=$DST_DIR/bigquery \
   $1/$2