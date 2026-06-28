#!/bin/bash
echo "Deleting rfx-core-1.0.jar"

rm BUILD-OUTPUT/rfx-core-1.0.jar

echo "Building rfx-core-1.0.jar"

gradle jar