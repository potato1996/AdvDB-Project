#!/bin/bash
# Author: Dayou Du (dayoudu@nyu.edu)
PROGRAM=${*:-./repcrec}
shift
INDIR=${1:-./inputs}
shift
OUTDIR=${1:-./outputs}
echo "program=<$PROGRAM> indir=<$INDIR> outdir=<$OUTDIR>"

INS="`seq 1 26`" 
INPRE="test"
OUTPRE="out"


############################################################################
#  NO TRACING 
############################################################################

for f in ${INS}; do
	echo "${PROGRAM} ${INDIR}/${INPRE}${f} > ${OUTDIR}/${OUTPRE}${f}"
	${PROGRAM} ${INDIR}/${INPRE}${f} > ${OUTDIR}/${OUTPRE}${f} &
done

