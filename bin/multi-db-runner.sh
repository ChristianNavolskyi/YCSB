#!/bin/bash
#
# Copyright (c) 2018 YCSB contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
#

dataSetBaseFolder=""
databases=('basic')
folderOptions=()
hdrProcessorPath=""
outputFolder=""
parameters=()
runTypes=("load" "run")
times=1
workloadFolder=""
workloadFiles=()

usage="Usage: $(basename "$0") -w workloadFolder <options>"
description="Program to run multiple databases with multiple workloads."
options="
Options:
    -d databaseName     name of the database to use. Can be used multiple times.
                        Default is 'basic'.
    -f folderOption     The different path option identifiers for the databases.
    -h                  show this help text.
    -l                  HDRLogProcessor path. Used to decode the measurements.
    -o outputFolder     path to a folder in which all measurements will be stored.
                        Default will use the current directory.
    -p parameter        Parameter for the databases. See in the bindings README.md for more information.
                        Don't set the path here use the '-f' option Multiple can be set.
    -s dataSetFolder    Folder to store the created data set in or use the present ones in the folder.
    -t times            number of times to run all benchmarks. Used to ensure consistency.
    -w workloadFolder   path to a folder with workloads in it.
                        All workloads in that directory will be used with all supplied databases.
                        workload, operationcount and recordcount or insertcount have to be set in that file to ensure that the execution terminates."


# Parse options
while getopts ':d:f:hl:o:p:s:t:w:' option; do
  case "$option" in
    d)  if [ ${databases[0]} == "basic" ]; then
            databases=($OPTARG)
        else
            databases+=($OPTARG)
        fi
        ;;
    f)  folderOptions+=($OPTARG)
        ;;
    h)  echo "$usage"
        echo "$description"
        echo "$options"
        exit 0
        ;;
    l)  if [ "$(basename ${OPTARG})" == "HistogramLogProcessor" ]; then
            hdrProcessorPath=${OPTARG}
        fi
        ;;
    o)  outputFolder=$OPTARG
        ;;
    p)  parameters+=($OPTARG)
        ;;
    s)  dataSetBaseFolder=$OPTARG
        ;;
    t)  if [ $OPTARG -gt 0 ]; then
            times=$OPTARG
        else
            echo "-t option has to be greater than 0."
            exit 1
        fi
        ;;
    w)  workloadFolder=$OPTARG
        ;;
    :)  echo "Missing argument for -$OPTARG" >&2
        exit 1
        ;;
   \?)  echo "Illegal option: -$OPTARG" >&2
        exit 1
        ;;
  esac
done
shift $((OPTIND - 1))

function progress()
{
    PARAM_PROGRESS=$1;
    PARAM_PHASE=$2;

    if [ ${PARAM_PROGRESS} -ge 0 ]; then echo -ne "[..........................] ($PARAM_PROGRESS%)  $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 5 ]; then echo -ne "[#.........................] ($PARAM_PROGRESS%)  $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 10 ]; then echo -ne "[###.......................] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 15 ]; then echo -ne "[####......................] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 20 ]; then echo -ne "[#####.....................] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 25 ]; then echo -ne "[#######...................] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 30 ]; then echo -ne "[########..................] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 35 ]; then echo -ne "[#########.................] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 40 ]; then echo -ne "[##########................] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 45 ]; then echo -ne "[############..............] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 50 ]; then echo -ne "[#############.............] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 55 ]; then echo -ne "[##############............] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 60 ]; then echo -ne "[################..........] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 65 ]; then echo -ne "[#################.........] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 70 ]; then echo -ne "[##################........] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 75 ]; then echo -ne "[####################......] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 80 ]; then echo -ne "[#####################.....] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 85 ]; then echo -ne "[######################....] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 90 ]; then echo -ne "[#######################...] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 95 ]; then echo -ne "[#########################.] ($PARAM_PROGRESS%) $PARAM_PHASE \033[0K\r" ; fi;
    if [ ${PARAM_PROGRESS} -ge 100 ]; then echo -e "$PARAM_PHASE \033[0K" ; PARAM_PROGRESS=0; fi;
}

# Check if workload folder is set and is a directory.
if [ -z ${workloadFolder} ] || [ ! -d ${workloadFolder} ]; then
    echo "Workload folder not set or is not a valid directory."
    exit 1
fi

# Check if workload folder contains valid workload files (workload, operationcount and one of insertcount or
# recordcount must be set).
shopt -s nullglob
files=(${workloadFolder}/*)
if [ "${#files[@]}" -eq 0 ]; then
    echo "No workload files supplied."
    exit 1
fi

currentDir=$(pwd)
cd ${workloadFolder}
shopt -s nullglob
workloadFiles=(`realpath *`)
shopt -u nullglob

for workloadFile in ${workloadFiles[@]}; do
    if ! (grep -q -c -m 1 "workload=" ${workloadFile} &&
    grep -q -c -m 1 "operationcount=[1-9]" ${workloadFile} &&
    ( grep -q -c -m 1 "recordcount=[1-9]" ${workloadFile} ||
      grep -q -c -m 1 "insertcount=[1-9]" ${workloadFile} )); then
        echo "$workloadFile is not a valid workload."
        exit 1
    fi
done

cd ${currentDir}


# Check if set databases are valid (contained in bindings.properties).
for database in ${databases[@]}; do
    if ! grep -q -c -m 1 "${database}:" bindings.properties; then
        echo "$database is not a valid database."
        exit 1
    fi
done


# Check number of path options equals number of databases.
if [[ ${databases[0]} != "basic" ]] && [ ${#databases[*]} -gt ${#folderOptions[*]} ]; then
    echo "Not all folder options supplied for all databases.
    Got ${#databases[*]} databases but ${#folderOptions[*]} folder options."
    exit 1
fi


# Create output folder for measurements if necessary.
if [ -z ${outputFolder} ]; then
    outputFolder="measurements"
    if [ ! -d ${outputFolder} ]; then
        mkdir ${outputFolder}
    fi
    echo "Output folder not set, will use $(realpath ${outputFolder}) for measurements."
elif [ ! -d ${outputFolder} ]; then
    echo "Creating output folder \"$outputFolder\"."
    mkdir ${outputFolder}
fi

# Create folder for data set
if [ -z ${dataSetBaseFolder} ]; then
    dataSetBaseFolder="${outputFolder}/dataSet"
fi
if [ ! -d ${dataSetBaseFolder} ]; then
    mkdir ${dataSetBaseFolder}
fi


# Print all set parameters
echo "
Databases:          ${databases[*]}
Workloads:          ${workloadFiles[*]}
Parameters:         ${parameters[*]}
Folder options:     ${folderOptions[*]}
Times to run:       $times
Output folder:      $outputFolder
Data set folder:    $dataSetBaseFolder
"


# Prepare the benchmark runs
parameterString=""
for parameter in ${parameters[*]}; do
    parameterString+="-p $parameter "
done

databaseFolder="${outputFolder}/database"
folderOptionString=""
for pathOption in ${folderOptions[*]}; do
    if [ "$pathOption" == "orientdb.url" ]; then
        folderOptionString+="-p $pathOption=plocal:$databaseFolder "
    elif [ "$pathOption" == "sparksee.path" ]; then
        folderOptionString+="-p $pathOption=$databaseFolder/database.gdb "
    else
        folderOptionString+="-p $pathOption=$databaseFolder "
    fi
done


# Execute benchmark
current=0
numOfTotalBenchmarkRuns=$(( ${#databases[@]} * ${#runTypes[@]} * ${#workloadFiles[@]} * ${times} ))

if [ -n "$hdrProcessorPath" ] && [ -e "$hdrProcessorPath" ]; then
    numOfTotalBenchmarkRuns=$(( ${numOfTotalBenchmarkRuns} * 2 ))
fi

numOfTotalBenchmarkRuns=$(( ${numOfTotalBenchmarkRuns} + ${#runTypes[@]} * ${#workloadFiles[@]} ))
sizeFile="${outputFolder}/databaseSizes.txt"

if [ ! -e ${sizeFile} ]; then
    touch ${sizeFile}
fi

echo "$(date)" >> ${sizeFile}

for workload in ${workloadFiles[*]}; do
    dataSetFolder="${dataSetBaseFolder}/$(basename ${workload})"

    # Create data set
    mkdir -p ${dataSetFolder}
    for runType in ${runTypes[*]}; do
        progress $(( current / numOfTotalBenchmarkRuns )) "$(basename ${workload}) $runType"
        ./ycsb.sh ${runType} basic -P ${workload} -p datasetdirectory=${dataSetFolder} &> /dev/null
        current=$(( $current + 100 ))
    done

    # Run benchmark
    for database in ${databases[*]}; do
        for i in $(seq 1 ${times}); do
            mkdir -p ${databaseFolder}

            for runType in ${runTypes[*]}; do
                measurementsFolder="$outputFolder/$database/$(basename ${workload})/$runType/$i"

                currentRunName="$database - $(basename ${workload}) - $i/$times - $runType"
                progress $(( current / numOfTotalBenchmarkRuns )) "$currentRunName"

                if [ ! -d ${measurementsFolder} ]; then
                    mkdir -p ${measurementsFolder}

                    ./ycsb.sh ${runType} ${database} -P ${workload} -s -p datasetdirectory=${dataSetFolder} \
                    ${parameterString} \
                    ${folderOptionString} \
                    -p measurement.trackjvm=true \
                    -p measurement.interval=both \
                    -p measurementtype=hdrhistogram \
                    -p hdrhistogram.fileoutput=true \
                    -p hdrhistogram.output.path=${measurementsFolder}/ \
                    -p exportfile=${measurementsFolder}/measure &> ${measurementsFolder}/consoleOutput.txt
                fi

                current=$(( $current + 100 ))

                if [ -n "$hdrProcessorPath" ] && [ -e "$hdrProcessorPath" ]; then
                    progress $(( current / numOfTotalBenchmarkRuns )) "Processing histograms of $currentRunName"

                    for measurement in ${measurementsFolder}/*.hdr; do
                        if [ -e "$measurement" ]; then
                            filename=$(basename ${measurement})

                            $(${hdrProcessorPath} -i ${measurement} \
                            -o ${measurementsFolder}/${filename%.*} \
                            -outputValueUnitRatio 1000)
                        fi
                    done

                    current=$(( $current + 100 ))
                fi

                echo "$(du -sh ${databaseFolder}) $currentRunName" >> ${sizeFile}
            done

            rm -rf ${databaseFolder}
        done
    done
done
progress $(( current / numOfTotalBenchmarkRuns )) "Done!"