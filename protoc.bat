@echo off
SET path=%1
"%~dp0\protoc\bin\protoc" --decode_raw <"%path%"
