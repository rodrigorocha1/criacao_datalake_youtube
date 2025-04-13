#!/bin/bash


apt-get update && apt-get install -y libkrb5-dev

pip install dbt-hive
