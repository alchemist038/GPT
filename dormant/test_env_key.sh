#!/bin/bash
set -e
source /media/sf_REC/keys/openai/env.sh
echo "DATE=$(date '+%Y-%m-%d %H:%M:%S')"
echo "OPENAI_API_KEY=${OPENAI_API_KEY:-NOT_SET}"
