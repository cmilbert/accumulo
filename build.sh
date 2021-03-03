#!/usr/bin/env bash

# Builds the whole project, then runs out s3 ReadWriteTest
mvn clean package install -DskipTests -Drat.skip=true -Dmdep.analyze.skip=true -Dspotbugs.skip=true

mvn test -Dtest=ReadWriteTest -DfailiFNoTest=false -Dcheckstyle.skip -Drat.skip=true -Dmdep.analyze.skip=true -Dspotbugs.skip=true
