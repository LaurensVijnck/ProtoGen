<<<<<<< HEAD
# export GOOGLE_APPLICATION_CREDENTIALS=/Users/jonnydaenen/tmp/datalayer-core-dev-tf.json
export GOOGLE_APPLICATION_CREDENTIALS="/Users/lvijnck/Documents/google-cloud-sdk/geometric-ocean-284614-77fba73ca7b0.json"

# Activate venv
VENV_PATH=/Users/lvijnck/Desktop/env/ProtoToBQ/bin/activate
#VENV_PATH=/Users/jonnydaenen/repos/github/ProtoGen/ProtoToBQ/venv/bin/activate
=======
export GOOGLE_APPLICATION_CREDENTIALS=/Users/jonnydaenen/tmp/datalayer-core-dev-tf.json

# Activate venv
VENV_PATH=/Users/jonnydaenen/repos/github/ProtoGen/ProtoToBQ/venv/bin/activate
>>>>>>> 425959e9ba6aa246d29360a1dbc8a7e404a87e40
source $VENV_PATH

# Add path to executable to path variable
# FUTURE: Create venv as part of the plugin
<<<<<<< HEAD
SRC_DIR=/Users/lvijnck/Desktop/ProtoBQGeneration/ProtoToBQ/
#SRC_DIR=/Users/jonnydaenen/repos/github/ProtoGen/ProtoToBQ
=======
SRC_DIR=/Users/jonnydaenen/repos/github/ProtoGen/ProtoToBQ
>>>>>>> 425959e9ba6aa246d29360a1dbc8a7e404a87e40
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