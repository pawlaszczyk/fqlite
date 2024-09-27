#!/usr/bin/env bash

DIR=$(cd "$(dirname "$0")"; pwd)
$DIR/protoc/bin/protoc --decode_raw < $1