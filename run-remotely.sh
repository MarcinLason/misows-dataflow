#!/usr/bin/env bash
mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=pl.edu.agh.misows.WordCount \
      -Dexec.args="--project=misows-dataflow-demo --stagingLocation=gs://misows-dataflow-demo/staging/ \
      --output=gs://misows-dataflow-demo/output/ --runner=DataflowRunner"