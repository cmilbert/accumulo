#!/bin/env bash

mvn compile -Dtest=Poop -DtestFailureIgnore=true verify -DfailiFNoTest=false -Dcheckstyle.skip -Drat.skip=true -Dmdep.analyze.skip=true -Dspotbugs.skip=true -Dfailsafe.groups=org.apache.accumulo.test.categories.MiniClusterOnlyTests -Dit.test=BulkNewIT -Dformatter.skip=true
