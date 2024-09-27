@echo off
SET path=%1
"%~dp0\protoc\bin\protoc.exe" --decode_raw <"%path%"