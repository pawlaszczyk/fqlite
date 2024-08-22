#!/usr/bin/env bash

DIR=$(cd "$(dirname "$0")"; pwd)
xattr -d com.apple.quarantine $DIR/protoc/bin/protoc
$DIR/protoc/bin/protoc --decode_raw < $1
