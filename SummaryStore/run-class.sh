#!/bin/bash
# Not meant to be invoked directly. Used as a template for scripts in each
# module; e.g. see distributed/run-server.sh

source $(dirname $0)/consts.sh

if [ $# -lt 3 ]
then
	echo "SYNTAX: $0 targetScript packageName className [classArgs]"
	exit 2
fi
BASEDIR=$(dirname $1)
PACKAGE=$2
CLASS=$3
shift 3

TARGETDIR="$BASEDIR/target"
if [ ! -d $TARGETDIR ]
then
    echo "Target directory $TARGETDIR not found"
    exit 1
fi
cp=$BASEDIR
for jar in $TARGETDIR/*.jar $TARGETDIR/*/*.jar
do
	cp="$cp:$jar"
done
if [ -d "$BASEDIR/scripts" ]
then
    cp="$cp:$BASEDIR/scripts" # put all scripts on classpath, so they can be found with class.getResource()
fi

java \
    -Dorg.slf4j.simpleLogger.showDateTime=true \
	-Dorg.slf4j.simpleLogger.showShortLogName=true \
	-Djava.rmi.server.hostname=localhost \
    -ea -Xmx$XMX -cp $cp \
    $PACKAGE.$CLASS $*
