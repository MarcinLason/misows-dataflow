#!/usr/bin/env bash
mvn compile exec:java -Dexec.mainClass=pl.edu.agh.misows.WordCount -Dexec.args=--output="./output/formspring##./output/beers##./output/politics##./output/reddit##./output/ubuntu"
