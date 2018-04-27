#!/bin/bash

workloadFolder=""
measurementsFolder=""
databases=()
workloadFiles=()
outputFile="evaluation.csv"

metaData=("database" "workloadName" "phase" "run")

workloadParameters=("recordcount" "operationcount" "productsperorder" "componentsperproduct" "testparametercount" "fieldlength" "insertproportion" "readproportion" "scanproportion" "updateproportion" "onlynodes" "index")

resultParameters=("overall time (ms)" "overall throughput (ops/sec)" "initialise average (ms)" "sys_load_avg min" "sys_load_avg max" "sys_load_avg average" "sys_load_avg StdD" "thread_count min" "thread_count max" "thread_count average" "thread_count StdD" "used_mem_mb min" "used_mem_mb max" "used_mem_mb average" "used_mem_mb StdD" "cleanup average" "insert min (us)" "insert max (us)" "insert average (us)" "insert StdD (us)" "insertNode min (us)" "insertNode max (us)" "insertNode average (us)" "insertNode StdD (us)" "insertEdge min (us)" "insertEdge max (us)" "insertEdge average (us)" "insertEdge StdD (us)" "read min (us)" "read max (us)" "read average (us)" "read StdD (us)" "scan min (us)" "scan max (us)" "scan average (us)" "scan StdD (us)")

while getopts 'd:m:w:' option; do
  case "$option" in
    d)  databases+=($OPTARG)
        ;;
    m)  measurementsFolder=$OPTARG
        ;;
    w)  workloadFolder=$OPTARG
        ;;
  esac
done

if [ -e ${databases} ]; then
  echo "No databases to look for"
  exit 1
fi

if [ -z ${workloadFolder} ] || [ ! -d ${workloadFolder} ] || [ -z ${measurementsFolder} ] || [ ! -d ${measurementsFolder} ]; then
  echo "Missing folders"
  exit 1
fi

outputFile=${measurementsFolder}/${outputFile}
if [ -e ${outputFile} ]; then
  rm ${outputFile}
fi
touch ${outputFile}
keyLine=""
for ((i = 0; i < ${#metaData[@]}; i++)); do
  keyLine+="${metaData[$i]};"
done
for ((i = 0; i < ${#workloadParameters[@]}; i++)); do
  keyLine+="${workloadParameters[$i]};"
done
for ((i = 0; i < ${#resultParameters[@]}; i++)); do
  keyLine+="${resultParameters[$i]};"
done

echo ${keyLine} >> ${outputFile}

workloadFiles=( $(find ${workloadFolder} -type f -print0 | while read -d $'\0' workload; do
  echo ${workload}
done) )

for workload in ${workloadFiles[*]}; do
  for database in ${databases[*]}; do
    resultFolder="${measurementsFolder}/${database}/$(basename ${workload})"

    if [ -d ${resultFolder} ]; then
      for phaseFolder in ${resultFolder}/*; do
        phase=$(basename ${phaseFolder})
        for runFolder in ${phaseFolder}/*; do
          run=$(basename ${runFolder})
          resultsToAdd="${database};$(basename ${workload});${phase};${run};"

          for workloadParameter in ${workloadParameters[*]}; do
            resultsToAdd+="$(cat ${workload} | grep ${workloadParameter} | cut -d = -f 2 | head -n 1);"
          done

          for ((i = 0; i < ${#resultParameters[@]}; i++)); do
            resultParameter=${resultParameters[$i]}
            firstPart="$(echo "${resultParameter}" | cut -d ' ' -f 1)"
            secondPart="$(echo "${resultParameter}" | cut -d ' ' -f 2)"

            if [[ "${secondPart}" == "StdD" ]]; then
              filename=$(ls ${runFolder} | grep -i -w "${firstPart}.hgrm" | grep -v Intended)

              if [ ! -d ${filename} ]; then
                resultsToAdd+="$(cat ${runFolder}/${filename} | grep StdD | cut -d , -f 2 | cut -d = -f 2 | tr ] ' ' | tr -d '[:space:]' | tr . ,);"
              fi
            else
              if [ -e ${runFolder}/measure ]; then
                  resultsToAdd+="$(cat ${runFolder}/measure | grep -i \\[${firstPart}\\] | grep -i ${secondPart} | cut -d , -f 3 | tr -d '[:space:]' | tr . ,);"
              fi
            fi
          done
          echo ${resultsToAdd} >> ${outputFile}
        done
      done
    fi
  done
done
