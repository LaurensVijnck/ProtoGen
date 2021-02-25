export GOOGLE_APPLICATION_CREDENTIALS=/Users/jonnydaenen/tmp/datalayer-core-dev-tf.json

# Activate venv
VENV_PATH=/Users/jonnydaenen/repos/github/ProtoGen/ProtoToBQ/venv/bin/activate
source $VENV_PATH

# Add path to executable to path variable
# FUTURE: Create venv as part of the plugin
SRC_DIR=/Users/jonnydaenen/repos/github/ProtoGen/ProtoToBQ
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

# Invoke Custom BigQuery pluging
protoc $1 \
  --experimental_allow_proto3_optional \
  --bq_out=$DST_DIR/bigquery