#!/bin/sh
JAVA_HOME=/usr
#JAVA_HOME=/usr/local/java
#JAVA_HOME="/usr/local/j2sdk1.4.2_07"
JAVAC=$JAVA_HOME/bin/javac
JAVA=$JAVA_HOME/bin/java
CLASSP="../bin:/usr/share/java/mysql.jar:../lib/libextractor.jar:../lib/metadata-extractor-2.3.1.jar"
RUNFLAGS="-Djava.library.path=/usr/local/lib -Dlibextractor.warn=1"
ROOT=$1
OUTDIR=./output

$JAVA -mx128M -cp $CLASSP $RUNFLAGS fl.archivist.Scan -p -d $ROOT/root > $OUTDIR/$ROOT.out

