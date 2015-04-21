#! /bin/sh


if [ "$#" -ne 2 ] || ! [ -d "$1" ]; then
  echo "Usage: $0 DATA_DIRECTORY SCALE_FACTOR" >&2
  exit 1
fi


DATA_ROOT=$1
SCALE=$2
BASEDIR=$(dirname $0)


mkdir $DATA_ROOT/scale_$SCALE
$BASEDIR/dbgen -T L -s $SCALE -v > gen_log.dat
mv $BASEDIR/*.tbl $DATA_ROOT/scale_$SCALE/


