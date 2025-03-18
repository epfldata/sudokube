#!/bin/bash
cat hosts | xargs -P8 -n1 scripts/sync.sh