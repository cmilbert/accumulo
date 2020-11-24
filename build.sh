#!/usr/bin/env bash

mvn clean install -DskipTests -Drat.skip=true -Dmdep.analyze.skip=true -Dspotbugs.skip=true
