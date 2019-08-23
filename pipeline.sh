#!/bin/bash -e

# module loads for programs
IMOD_VERSION="4.9.11"
IMOD_LOAD="imod/${IMOD_VERSION}"
EMAN2_VERSION="20190603"
EMAN2_LOAD="eman2/${EMAN2_VERSION}"
MOTIONCOR2_VERSION="1.1.0"
MOTIONCOR2_LOAD="motioncor2-1.1.0-gcc-4.8.5-zhoi3ww"
#MOTIONCOR2_VERSION="1.2.2"
#MOTIONCOR2_LOAD="motioncor2/${MOTIONCOR2_VERSION}"
CTFFIND4_VERSION="4.1.10"
CTFFIND4_LOAD="ctffind/${CTFFIND4_VERSION}"
RELION_VERSION="3.0.4"
RELION_LOAD="relion/${RELION_VERSION}"
IMAGEMAGICK_VERSION="6.8.9"
IMAGEMAGICK_LOAD="imagemagick/$IMAGEMAGICK_VERSION"

# GENERATE
# force redo of all files
MODE=${MODE:-spa} # spa | tomo
TASK=${TASK:-all}
FORCE=${FORCE:-0}
NO_FORCE_GAINREF=${NO_FORCE_GAINREF:-0}
NO_PREAMBLE=${NO_PREAMBLE:-0}

# SCOPE PARAMS
CS=${CS:-2.7}
KV=${KV:-300}
APIX=${APIX}
SUPERRES=${SUPERRES:-0}
PHASE_PLATE=${PHASE_PLATE:-0}
AMPLITUDE_CONTRAST=${AMPLITUDE_CONTRAST:-0.1}

# MOTIONCOR2 PARAMETERS
BFT=${BFT:-150}
FMDOSE=${FMDOSE}
PATCH=${PATCH:-5 5}
THROW=${THROW:-0}
TRUNC=${TRUNC:-0}
ITER=${ITER:-10}
TOL=${TOL:-0.5}
OUTSTACK=${OUTSTACK:-0}
INFMMOTION=${INFMMOTION:-1}
GPU=${GPU:-0}
GPU_OFFSET=${GPU_OFFSET:-0}
FMREF=${FMREF:-0}
INITDOSE=${INITDOSE:-0}

# PICKING
PARTICLE_SIZE=${PARTICLE_SIZE:-150}
PARTICLE_SIZE_MIN=${PARTICLE_SIZE_MIN:-$(echo $PARTICLE_SIZE | awk '{ print $1*0.8 }')}
PARTICLE_SIZE_MAX=${PARTICLE_SIZE_MAX:-$(echo $PARTICLE_SIZE | awk '{ print $1*1.2 }')}

# usage
usage() {
  cat <<__EOF__
Usage: $0 MICROGRAPH_FILE

Mandatory Arguments:
  [-a|--apix FLOAT]            use specified pixel size
  [-d|--fmdose FLOAT]          use specified fmdose in calculations

Optional Arguments:
  [-g|--gainref GAINREF_FILE]  use specificed gain reference file
  [-b|--basename STR]          output files names with specified STR as prefix
  [-k|--kev INT]               input micrograph was taken with INT keV microscope
  [-s|--superres]              input micrograph was taken in super-resolution mode (so we should half the number of pixels)
  [-p|--phase-plate]            input microgrpah was taken using a phase plate (so we should calculate the phase)
  [-P|--patch STRING]          use STRING patch settings for motioncor2 alignment
  [-e|--particle-size INT]     pick particles with size INT
  [-f|--force]                 reprocess all steps (ignore existing results).
  [-m|--mode [spa|tomo]]       pipeline to use: single particle analysis of tomography
  [-t|--task sum|align|pick|all] what to process; sum the stack, align the stack; just particle pick or all
  
__EOF__
}

# determine what to run
main() {

  # map long arguments to short
  for arg in "$@"; do
    shift
    case "$arg" in
      "--help")    set -- "$@" "-h";;
      "--gainref") set -- "$@" "-g";;
      "--basename") set -- "$@" "-b";;
      "--force")   set -- "$@" "-F";;
      "--apix")    set -- "$@" "-a";;
      "--fmdose")  set -- "$@" "-d";;
      "--kev")     set -- "$@" "-k";;
      "--superres") set -- "$@" "-s";;
      "--phase-plate") set -- "$@" "-p";;
      "--patch")   set -- "$@" "-P";;
      "--particle-size")   set -- "$@" "-e";;
      "--mode")    set -- "$@" "-m";;
      "--task")    set -- "$@" "-t";;
      *)           set -- "$@" "$arg";;
   esac
  done

  while getopts "Fhspm:t:g:b:a:d:k:e:" opt; do
    case "$opt" in
    g) GAINREF_FILE="$OPTARG";;
    b) BASENAME="$OPTARG";;
    a) APIX="$OPTARG";;
    d) FMDOSE="$OPTARG";;
    k) KV="$OPTARG";;
    s) SUPERRES=1;;
    p) PHASE_PLATE=1;;
    P) PATCH="$OPTARG";;
    e) PARTICLE_SIZE="$OPTARG";;
    F) FORCE=1;;
    m) MODE="$OPTARG";;
    t) TASK="$OPTARG";;
    h) usage; exit 0;;
    ?) usage; exit 1;;
    esac
  done

  MICROGRAPHS=${@:$OPTIND}
  # >&2 echo "MICROGRAPHS: ${MICROGRAPHS}"
  if [ ${#MICROGRAPHS[@]} -lt 1 ]; then
    echo "Need input micrograph MICROGRPAH_FILE to continue..."
    usage
    exit 1
  fi
  if [ -z $APIX ]; then
    echo "Need pixel size [-a|--apix] to continue..."
    usage
    exit 1
  fi
  if [[ -z $FMDOSE && ( "$TASK" == "all" || "$TASK" == "align" || "$TASK" == "sum" ) ]]; then
    echo "Need fmdose [-d|--fmdose] to continue..."
    usage
    exit 1
  fi

  for MICROGRAPH in ${MICROGRAPHS}; do

    if [ "$MODE" == "spa" ]; then
      >&2 echo "MICROGRAPH: ${MICROGRAPH}"
      do_spa
    elif [ "$MODE" == "tomo" ]; then
      do_tomo
    else
      echo "Unknown MODE $MODE"
      usage
      exit 1
    fi

  done
}



do_spa()
{

  if [ ${NO_PREAMBLE} -eq 0  ]; then
    do_prepipeline

    if [[ "$TASK" == "align" || "$TASK" == "sum" || "$TASK" == "all" ]]; then
      local force=${FORCE}
      if [ ${NO_FORCE_GAINREF} -eq 1 ]; then 
        FORCE=0
      fi
      do_gainref
      FORCE=$force
    fi
  else
    # still need to determine correct gainref
    local force=${FORCE}
    FORCE=0
    GAINREF_FILE=$(process_gainref "$GAINREF_FILE") || exit $?
    FORCE=$force
  fi

  # start doing something!
  echo "single_particle_analysis:"

  if [[ "$TASK" == "align" || "$TASK" == "all" ]]; then
    do_spa_align
  fi
  if [[ "$TASK" == "sum" || "$TASK" == "all" ]]; then
    do_spa_sum
  fi

  if [[ "$TASK" == "pick" || "$TASK" == "all" ]]; then
    # get the assumed pick file name
    if [ -z $ALIGNED_FILE ]; then
      ALIGNED_FILE=$(align_file ${MICROGRAPH})  || exit $?
    fi
    do_spa_pick
  fi

  if [[ "$TASK" == "preview" || "$TASK" == "all" ]]; then
    echo "  - task: preview"
    local start=$(date +%s.%N)
    # need to guess filenames
    if [ "$TASK" == "preview" ]; then
      ALIGNED_DW_FILE=$(align_dw_file ${MICROGRAPH}) || exit $?
      #echo "ALIGNED_DW_FILE: $ALIGNED_DW_FILE"
      ALIGNED_CTF_FILE=$(align_ctf_file "${MICROGRAPH}") || exit $?
      #echo "ALIGNED_CTF_FILE: $ALIGNED_CTF_FILE"
      PARTICLE_FILE=$(particle_file ${ALIGNED_DW_FILE}) || exit $?
      #echo "PARTICLE_FILE: $PARTICLE_FILE"
      SUMMED_CTF_FILE=$(sum_ctf_file "${MICROGRAPH}") || exit $?
      # remove the _sum bit if SUMMED_FILE defined
      if [ ! -z $SUMMED_FILE ]; then
        SUMMED_CTF_FILE="${SUMMED_CTF_FILE%_sum_ctf.mrc}_ctf.mrc"
        #>&2 echo "SUMMED CTF: $SUMMED_CTF_FILE"
      fi
      #echo "SUMMED_CTF_FILE: $SUMMED_CTF_FILE"
    fi
    local PREVIEW_FILE=$(generate_preview) || exit $?
    echo "    files:"
    dump_file_meta "${PREVIEW_FILE}"
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  fi

}

do_tomo()
{

  echo "TODO"
  exit 255

}


do_prepipeline()
{
  echo "pre-pipeline:"

  # other params
  echo "  - task: input"
  echo "    data:"
  echo "      apix: ${APIX}"
  echo "      fmdose: ${FMDOSE}"
  echo "      astigmatism: ${CS}"
  echo "      kev: ${KV}"
  echo "      amplitude_contrast: ${AMPLITUDE_CONTRAST}"
  echo "      super_resolution: ${SUPERRES}"
  echo "      phase_plate: ${PHASE_PLATE}"

  # input micrograph
  echo "  - task: micrograph"
  echo "    files:"
  dump_file_meta "${MICROGRAPH}" || exit $?

}


do_gainref()
{

  if [ ! -z "$GAINREF_FILE" ]; then
    # gainref
    echo "  - task: convert_gainref"
    local start=$(date +%s.%N)
    GAINREF_FILE=$(process_gainref "$GAINREF_FILE") || exit $?
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
    echo "    files:"
    dump_file_meta "${GAINREF_FILE}" || exit $?
  fi
}


###
# process the micrograph by aligning and creating the ctfs and previews for the MICROGRAPH
###
do_spa_align() {

  >&2 echo
  >&2 echo "Processing align for micrograph $MICROGRAPH..."

  echo "  - task: align_stack"
  local start=$(date +%s.%N)
  ALIGNED_FILE=$(align_stack "$MICROGRAPH" "$GAINREF_FILE") || exit $? #"./aligned/motioncor2/$MOTIONCOR2_VERSION")
  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  echo "    duration: $duration"
  echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  echo "    files:"
  dump_file_meta "${ALIGNED_FILE}" || exit $?
  ALIGNED_DW_FILE="${ALIGNED_FILE%.mrc}_DW.mrc"
  dump_file_meta "${ALIGNED_DW_FILE}" || exit $?

  echo "  - task: align_data"
  local file=$(motioncor_file ${ALIGNED_FILE}) || exit $?
  echo "    source: $file"
  echo "    data:"
  local align=$(parse_motioncor ${ALIGNED_FILE}) || exit $?
  eval $align || exit $?
  for k in "${!align[@]}"; do
  echo "      $k: ${align[$k]}"
  done

  PROCESSED_ALIGN_FIRST1=${align[first1]}
  PROCESSED_ALIGN_FIRST3=${align[first3]}
  PROCESSED_ALIGN_FIRST5=${align[first5]}
  PROCESSED_ALIGN_FIRST8=${align[first8]}
  PROCESSED_ALIGN_ALL=${align[all]}

  # create a file that relion can read
  local star=$(create_motioncor_star ${ALIGNED_FILE}) || exit $?

  echo "  - task: ctf_align"
  local start=$(date +%s.%N)
  # we always bin down the aligned file if superres, so we need to prevent the ctf from using the wrong apix value
  local orig_superres=${SUPERRES}
  SUPERRES=0
  local outdir="aligned/motioncor2/$MOTIONCOR2_VERSION/ctffind4/$CTFFIND4_VERSION"
  ALIGNED_CTF_FILE=$(process_ctffind "$ALIGNED_FILE" "$outdir") || exit $?
  SUPERRES=${orig_superres}
  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  echo "    duration: $duration"
  echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  echo "    files:"
  dump_file_meta  "${ALIGNED_CTF_FILE}" || exit $?

  echo "  - task: ctf_align_data"
  local ctf_file=$(ctffind_file $ALIGNED_CTF_FILE) || exit $?
  echo "    source: $ctf_file"
  local ctf_data=$(parse_ctffind $ALIGNED_CTF_FILE) || exit $?
  eval $ctf_data || exit $?
  echo "    data:"
  for k in "${!ctf[@]}"; do
  echo "      $k: ${ctf[$k]}"
  done

  PROCESSED_ALIGN_RESOLUTION=${ctf[resolution]}
  PROCESSED_ALIGN_RESOLUTION_PERFORMANCE=${ctf[resolution_performance]}
  PROCESSED_ALIGN_ASTIGMATISM=${ctf[astigmatism]}
  PROCESSED_ALIGN_CROSS_CORRELATION=${ctf[cross_correlation]}
}

sum_ctf_file()
{
  local input="$1"
  if [ ! -z "${BASENAME}" ]; then
    input="${BASENAME}"
  fi
  local extension="${input##*.}"
  local outdir=${2:-summed/imod/$IMOD_VERSION/ctffind4/$CTFFIND4_VERSION}
  local output="$outdir/${input%.${extension}}_sum_ctf.mrc"
  echo $output
}

align_ctf_file()
{
  local input="$1"
  if [ ! -z "${BASENAME}" ]; then
    input="${BASENAME}"
  fi
  local extension="${input##*.}"
  local outdir=${2:-aligned/motioncor2/$MOTIONCOR2_VERSION/ctffind4/$CTFFIND4_VERSION}
  local output="$outdir/${input%.${extension}}_aligned_ctf.mrc"
  echo $output
}

###
# create the ctf and preview images for the summed stack of the MICROGRAPH. creating the sum temporarily if necessary
###
do_spa_sum() {

  >&2 echo
  >&2 echo "Processing sum for micrograph $MICROGRAPH..."

  SUMMED_CTF_FILE=$(sum_ctf_file "$MICROGRAPH") || exit $?
  # check for the SUMMED_CTF_FILE, do if not exists
  if [ -e $SUMMED_CTF_FILE ]; then
    >&2 echo
    >&2 echo "sum ctf file $SUMMED_CTF_FILE already exists"
  fi

  local summed_file_log=""
  # work out the summed average fo teh stack if necessary
  local create_summed_file=1
  if [ ! -z $SUMMED_FILE ]; then
    create_summed_file=0
    >&2 echo "using summed micrograph $SUMMED_FILE..."
  fi
  if [[ -z $SUMMED_FILE && ( $FORCE -eq 1 || ! -e $SUMMED_CTF_FILE ) ]]; then
    echo "  - task: sum"
    local start=$(date +%s.%N)
    local file=$(basename -- "$SUMMED_CTF_FILE") || exit $?
    local tmpfile="/tmp/${file%_ctf.mrc}.mrc"
    SUMMED_FILE=$(process_sum "$MICROGRAPH" "$tmpfile" "$GAINREF_FILE") || exit $?
    summed_file_log="${SUMMED_FILE%.mrc}.log"
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  fi

  echo "  - task: ctf_summed"
  local start=$(date +%s.%N)
  if [[ $FORCE -eq 1 || ! -e "$SUMMED_CTF_FILE" ]]; then
    local outdir=$(dirname "$SUMMED_CTF_FILE") || exit $?
    SUMMED_CTF_FILE=$(process_ctffind "$SUMMED_FILE" "$outdir") || exit $?
    if [ $create_summed_file -eq 1 ]; then
      #>&2 echo "DELETING $SUMMED_FILE"
      rm -f "$SUMMED_FILE"
    fi
  fi
  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  echo "    duration: $duration"
  echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  echo "    files:"
  dump_file_meta "${SUMMED_CTF_FILE}" || exit $?

  if [ "$summed_file_log" != "" ]; then
    rm -f $SUMMED_FILE $summed_file_log || exit $?
  fi

  echo "  - task: ctf_summed_data"
  local ctf_file=$(ctffind_file $SUMMED_CTF_FILE) || exit $?
  echo "    source: $ctf_file"
  local ctf_data=$(parse_ctffind $SUMMED_CTF_FILE) || exit $?
  eval $ctf_data
  echo "    data:"
  for k in "${!ctf[@]}"; do
  echo "      $k: ${ctf[$k]}"
  done

  PROCESSED_SUM_RESOLUTION=${ctf[resolution]}
  PROCESSED_SUM_RESOLUTION_PERFORMANCE=${ctf[resolution_performance]}

}


do_spa_pick()
{
  # use DW file?
  >&2 echo

  if [ -z $ALIGNED_DW_FILE ]; then
    ALIGNED_DW_FILE=$(align_dw_file "$MICROGRAPH") || exit $?
  fi 

  >&2 echo "Processing particle picking for micrograph $ALIGNED_DW_FILE..."

  echo "  - task: particle_pick"
  local start=$(date +%s.%N)
  PARTICLE_FILE=$(particle_pick "$ALIGNED_DW_FILE") || exit $?
  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  echo "    duration: $duration"
  echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  echo "    files:"
  dump_file_meta "${PARTICLE_FILE}" || exit $?

  echo "    data:"
  # 11 non-particle lines
  local particles=$(wc -l ${PARTICLE_FILE} | awk '{print $1-11}') || exit $?
  PROCESSED_NUMBER_PARTICLES=$particles
  echo "      particles: " $particles

}



function gen_template() {
  eval "echo \"$1\""
}

process_gainref()
{
  # read in a file and spit out the appropriate gainref to actually use via echo as path
  local input=$1
  local outdir=${2:-.}
  if [[ ${input:0:1} == "/" ]]; then outdir=""; else mkdir -p $outdir; fi

  >&2 echo
  
  local filename=$(basename -- "$input")
  local extension="${filename##*.}"
  local output="$outdir/${filename}"

  if [ ! -e "$input" ]; then
    >&2 echo "gainref file $input does not exist!"
    exit 4
  fi

  if [[ "$extension" -eq "dm4" ]]; then
  
    output="$outdir/${input%.$extension}.mrc"
    if [[ $FORCE -eq 1 || ! -e $output ]]; then
      >&2 echo "converting gainref file $input to $output..."
      module load ${IMOD_LOAD} || exit $?
      dm2mrc "$input" "$output"  1>&2 || exit $?
    else
      >&2 echo "gainref file $output already exists"
    fi
  
  # TODO: this needs testing
  elif [[ "$extension" -eq 'mrc' && ! -e $output ]]; then
    
    >&2 echo "Error: output gainref file $output does not exist"
    
  fi
  
  echo $output
}


align_file()
{
  local input="$1"
  if [ ! -z "${BASENAME}" ]; then
    input="${BASENAME}"
  fi
  local filename=$(basename -- "$input")
  local outdir=${2:-aligned/motioncor2/$MOTIONCOR2_VERSION}
  local extension="${filename##*.}"
  local output="$outdir/${filename%.${extension}}_aligned.mrc"

  echo $output
}

align_dw_file()
{
  local align=$(align_file "$1")
  local output="${align%_aligned.mrc}_aligned_DW.mrc"
  echo $output
}

align_stack()
{
  # given a set of parameters, run motioncor on the input movie stack
  local input=$1
  local gainref="$2"
  local outdir=${3:-aligned/motioncor2/$MOTIONCOR2_VERSION}
  
  >&2 echo
  
  mkdir -p $outdir
  local output=$(align_file $input $outdir) || exit $?

  if [ -e $output ]; then
    >&2 echo "aligned file $output already exists"
  fi
  
  if [[ $FORCE -eq 1 || ! -e $output ]]; then

    local extension="${input##*.}"
    >&2 echo "aligning $extension stack $input to $output, using gainref file $gainref..."
    local gpu=$(($GPU+$GPU_OFFSET))
    local cmd="
      MotionCor2  \
        $(if [ "$extension" == 'mrc' ]; then echo '-InMrc'; else echo '-InTiff'; fi) '$input' \
        $(if [ ! '$gainref' == '' ]; then echo -Gain \'$gainref\'; fi) \
        -OutMrc $output \
        -LogFile ${output%.${extension}}.log \
        -FmDose $FMDOSE \
        -kV $KV \
        -Bft $BFT \
        -PixSize $(echo $APIX | awk -v superres=$SUPERRES '{ if( superres=="1" ){ print $1/2 }else{ print $1 } }') \
        -FtBin $(if [ $SUPERRES -eq 1 ]; then echo 2; else echo 1; fi) \
        -Patch $PATCH \
        -Throw $THROW \
        -Trunc $TRUNC \
        -InitDose $INITDOSE \
        -FmRef $FMREF \
        -Iter $ITER \
        -Tol $TOL \
        -OutStack $OUTSTACK \
        -InFmMotion $INFMMOTION \
        -Gpu $gpu \
        -GpuMemUsage 0.95
    "

    align_command=$(gen_template "$cmd") || exit $?
    >&2 echo "executing:" $align_command
    module load ${MOTIONCOR2_LOAD} || exit $?
    eval $align_command  1>&2 || exit $?
  fi

  echo $output
}


process_ctffind()
{
  # do a ctf of the input mrc
  local input=$1
  local outdir=${2:-.}
  
  >&2 echo

  if [ ! -e "$input" ]; then
    >&2 echo "input micrograph $input not found"
    exit 4
  fi
    
  local filename=$(basename -- "$input")
  local extension="${filename##*.}"
  local output="$outdir/${filename%.${extension}}_ctf.mrc"
  mkdir -p $outdir

  local apix=${APIX}
  if [[ $SUPERRES -eq 1 ]]; then
    apix=$(echo $apix | awk '{ print $1/2 }') || exit $?
  fi

  if [ -e $output ]; then
    >&2 echo "output ctf file $output already exists"
  fi

  if [[ $FORCE -eq 1 || ! -e $output ]]; then
    >&2 echo "ctf'ing micrograph $input to $output..."
    module load ${CTFFIND4_LOAD} || exit $?
    # phase plate?
    if [ $PHASE_PLATE -eq 0 ]; then
      ctffind > ${output%.${extension}}.log << __CTFFIND_EOF__
$input
$output
$apix
$KV
$CS
0.1
512
30
4
1000
50000
200
no
no
yes
100
no
no
__CTFFIND_EOF__
    else
      ctffind > ${output%.${extension}}.log << __CTFFIND_EOF__
$input
$output
$apix
$KV
$CS
0.1
512
30
4
1000
50000
200
no
no
yes
100
yes
0
1.571
0.1
no
__CTFFIND_EOF__
    fi
  fi

  if [ $? -ne 0 ]; then
    exit 1
  fi

  echo $output
}


generate_jpg()
{
  local input=$1
  local outdir=${2:-.}
  local lowpass=${3:-}
  
  >&2 echo

  if [ ! -e $input ]; then
    >&2 echo "input micrograph $input not found"
    exit
  fi
    
  local filename=$(basename -- "$input")
  local extension="${filename##*.}"
  local output="$outdir/${filename%.${extension}}.jpg"
  mkdir -p $outdir
  
  if [ -e $output ]; then
    >&2 echo "preview file $output already exists"
  fi

  if [[ $FORCE -eq 1 || ! -e $output ]]; then
    >&2 echo "generating preview of $input to $output..."
    module load ${EMAN2_LOAD} || exit $?
    if [ "$lowpass" == "" ]; then
      e2proc2d.py --writejunk $input $output  1>&2 || exit $?
    else
      e2proc2d.py --writejunk $input $output --process filter.lowpass.gauss:cutoff_freq=$lowpass  1>&2 || exit $?
    fi
  fi

  if [ ! -e $output ]; then
   >&2 echo "could not generate preview file $output!"
    exit 4
  fi

  echo $output
}


process_sum()
{
  local input=$1
  local output=$2
  # TODO: what if no gainref?
  local gainref=$3
  local outdir=${4:-.} #-$(sum_ctf_file ${MICROGRAPH})}
  
  local filename=$(basename -- "$output")
  local extension="${filename##*.}"
  local log="${output%.${extension}}.log"
  
  >&2 echo

  if [ ! -e $input ]; then
    >&2 echo "input micrograph $input not found"
    exit
  fi
    
  if [ -e $output ]; then
    >&2 echo "output micrograph $output already exists"
  fi

  if [[ $FORCE -eq 1 || ! -e $output ]]; then
    >&2 echo "summing stack $input to $output..."
    local tmpfile=$(mktemp /tmp/pipeline-sum.XXXXXX) || exit $?
    module load ${IMOD_LOAD} || exit $?
    >&2 echo "avgstack $input $tmpfile /"
    avgstack > $log << __AVGSTACK_EOF__
$input
$tmpfile
/
__AVGSTACK_EOF__
    >&2 echo clip mult -n 16 $tmpfile \'$gainref\' \'$output\'
    module load ${IMOD_LOAD} || exit $?
    clip mult -n 16 $tmpfile "$gainref" "$output"  1>&2 || exit $?
    rm -f $tmpfile || exit $?
  fi

  echo $output
}

particle_file()
{
  local input="$1"
  local dirname=${2:-particles}
  local extension="${input##*.}"
  local output="$dirname/${input%.${extension}}_autopick.star"
  echo $output
}

particle_pick()
{
  local input=$1
  local dirname=${2:-particles}

  >&2 echo

  if [ ! -e $input ]; then
    >&2 echo "input micrograph $input not found"
    exit 4
  fi

  local output=$(particle_file "$input") || exit $?

  if [ -e $output ]; then
    >&2 echo "particle file $output already exists"
  fi
  if [[ $FORCE -eq 1 || ! -e $output ]]; then

    >&2 echo "particle picking from $input to $output..."
    >&2  echo module load ${RELION_LOAD}
    module load ${RELION_LOAD} || exit $?
    local cmd="relion_autopick --i $input --odir $dirname/ --pickname autopick --LoG  --LoG_diam_min $PARTICLE_SIZE_MIN --LoG_diam_max $PARTICLE_SIZE_MAX --angpix $APIX --shrink 0 --lowpass 15 --LoG_adjust_threshold -0.1"
    >&2 echo $cmd 
    $cmd 1>&2 || exit $?

  fi

  echo $output
}

generate_file_meta()
{
  local file="$1"
  if [ -h "$file" ]; then
    file=$(realpath "$file") || exit $?
  fi
  if [ ! -e "$file" ]; then
    >&2 echo "file $file does not exist!"
    exit 4
  fi
  local md5file="$1.md5"
  if [ -e "$md5file" ]; then
    >&2 echo "md5 checksum file $md5file already exists..."
  fi
  local md5=""
  >&2 echo "calculating checksum and stat for $file..."
  if [[ $FORCE -eq 1 || ! -e $md5file ]]; then
    md5=$(md5sum "$1" | tee "$md5file" | awk '{print $1}' ) || exit $?
  else
    md5=$(cat "$md5file" | cut -d ' ' -f 1) || exit $?
  fi
  local stat=$(stat -c "%s/%y/%w" "$file") || exit $?
  local mod=$(date --utc -d "$(echo $stat | cut -d '/' -f 2)"  +%FT%TZ) || exit $?
  local create=$(echo $stat | cut -d '/' -f 3) || exit $?
  if [ "$create" == "-" ]; then create=$mod; fi
  local size=$(echo $stat | cut -d '/' -f 1) || exit $?
  echo "file=\"$1\" checksum=$md5 size=$size modify_timestamp=$mod create_timestamp=$create"
}

dump_file_meta()
{
  echo "      - path: $1"
  local out=$(generate_file_meta "$1") || exit $?
  eval "$out"
  echo "        checksum: $checksum"
  echo "        size: $size"
  echo "        modify_timestamp: $modify_timestamp"
  echo "        create_timestamp: $create_timestamp"
}

generate_preview()
{
  local outdir=${1:-previews}
  mkdir -p $outdir
  local filename=$(basename -- "$MICROGRAPH") || exit $?
  if [ ! -z ${BASENAME} ]; then
    filename="${BASENAME}_sidebyside.jpg"
  fi
  local extension="${filename##*.}"
  local output="$outdir/${filename}"

  # create a preview of the image
  # create the picked preview
  #local picked_preview=/tmp/tst.jpg
  local picked_preview=$(mktemp /tmp/pipeline-picked-XXXXXXXX.jpg) || exit $?

  if [ -e "$picked_preview" ]; then
    >&2 echo "particle picked preview file $picked_preview already exists..."
  fi

  if [ ! -e "$ALIGNED_DW_FILE" ]; then
    >&2 echo "aligned file $ALIGNED_DW_FILE not found..."
    exit 4
  fi
  local aligned_jpg=$(generate_jpg "$ALIGNED_DW_FILE" /tmp 0.05) || exit $?
  if [ ! -e "$PARTICLE_FILE" ]; then
    >&2 echo "particle file $PARTICLE_FILE not found..."
    exit 4
  fi

  module load ${IMAGEMAGICK_LOAD} || exit $?

  #if [ ! -e "$picked_preview" ]; then
    local origifs=$IFS
    IFS=$'
'
    local cmd="convert -flip -negate '$aligned_jpg' -strokewidth 3 -stroke yellow -fill none "
    local size=$( echo "$PARTICLE_SIZE * $APIX" | awk '{ print $1 }' ) || exit $?
    local i=0
    for l in $(cat $PARTICLE_FILE | grep -vE '(_|\#|^ $)' ); do
      local shape=$(echo $l | awk -v size=$size '{print "circle " $1 "," $2 "," $1 + size/2 "," $2 }')
      #cmd="${cmd} -strokewidth 3 -stroke yellow -fill none -draw \" $shape \" "
      cmd="${cmd} -draw \"$shape\" "
      i=$((i+1))
      #if [ $i -gt 10 ]; then
      #  break;
      #fi
    done
    cmd="${cmd}  $picked_preview"
    IFS=$origifs
    #>&2 echo $cmd
    eval $cmd || exit $?
  #fi
  PROCESSED_NUMBER_PARTICLES=${PROCESSED_NUMBER_PARTICLES:-$i}

  # get a timestamp of when file was created
  local timestamp=$(TZ=America/Los_Angeles date +"%Y-%m-%d %H:%M:%S" -r ${MICROGRAPH}) || exit $?

  # create the top half
  if [ ! -e "$SUMMED_CTF_FILE" ]; then
    >&2 echo "summed ctf file $SUMMED_CTF_FILE not found..."
    exit 4
  fi
  SUMMED_CTF_PREVIEW=$(generate_jpg "${SUMMED_CTF_FILE}" "/tmp" ) || exit $?

  local top=$(mktemp /tmp/pipeline-top-XXXXXXXX.jpg)
  local res="$(printf '%.1f' ${PROCESSED_SUM_RESOLUTION:-0.0})Å ($(echo ${PROCESSED_SUM_RESOLUTION_PERFORMANCE:-0.0} | awk '{printf( "%2.0f", $1*100 )}')%)" || exit $?
  #>&2 echo "ctf res $res"
  convert \
    -resize '512x512^' -extent '512x512' $picked_preview \
    -flip ${SUMMED_CTF_PREVIEW} \
    +append -font Helvetica-Narrow -pointsize 28 -fill SeaGreen1 -draw "text 8,502 \"~$PROCESSED_NUMBER_PARTICLES pp\"" \
    +append -font Helvetica-Narrow -pointsize 28 -fill yellow -draw "text 524,502 \"${timestamp}\"" \
    +append -font Helvetica-Narrow -pointsize 28 -fill yellow -draw "text 894,502 \"$res\"" \
    $top \
  || exit $?

  rm -f $picked_preview $SUMMED_CTF_PREVIEW || exit $?

  # create the bottom half
  if [ ! -e "$ALIGNED_CTF_FILE" ]; then
    >&2 echo "aligned ctf file $ALIGNED_CTF_FILE not found..."
    exit 4
  fi
  ALIGNED_CTF_PREVIEW=$(generate_jpg "${ALIGNED_CTF_FILE}" "/tmp" ) || exit $?
  local bottom=$(mktemp /tmp/pipeline-bottom-XXXXXXXX.jpg) || exit $?
  local res="$(printf '%.1f' ${PROCESSED_ALIGN_RESOLUTION:-0.0})Å ($(echo ${PROCESSED_ALIGN_RESOLUTION_PERFORMANCE:-0.0} | awk '{printf( "%2.0f", $1*100)}')%)"
  local ctf="cs $(printf '%.2f' ${PROCESSED_ALIGN_ASTIGMATISM:-0.0}) cc $(printf '%.2f' ${PROCESSED_ALIGN_CROSS_CORRELATION:-0.0})"
  local drift="$(printf "%.2f" ${PROCESSED_ALIGN_FIRST1:-0.0}) "/" $(printf "%.2f" ${PROCESSED_ALIGN_FIRST5:-0.0}) "/" $(printf "%.2f" ${PROCESSED_ALIGN_ALL:-0.0})"
 # >&2 echo "RES: $res DRIFT: $drift"
  convert \
    -resize '512x512^' -extent '512x512' \
    $aligned_jpg \
    ${ALIGNED_CTF_PREVIEW} \
    +append -font Helvetica-Narrow -pointsize 28 -fill orange -draw "text 334,30 \"$drift\"" \
    +append -font Helvetica-Narrow -pointsize 28 -fill orange -draw "text 524,30 \"$ctf\"" \
    +append -font Helvetica-Narrow -pointsize 28 -fill orange -draw "text 894,30 \"$res\"" \
    $bottom \
 || exit $?

  # clean files
  rm -f $aligned_jpg $ALIGNED_CTF_PREVIEW || exit $?

  convert $top $bottom \
     -append $output \
  || exit $?
  rm -f $top $bottom  || exit $?

  echo $output
}


ctffind_file()
{
  local input=$1

  local filename=$(basename -- "$input")
  local extension="${filename##*.}"
  local datafile="${input%.${extension}}.txt"

  echo $datafile
}


parse_ctffind()
{
  local input=$1
  local datafile=$(ctffind_file "$input") || exit $?

  if [ ! -e $datafile ]; then
    >&2 echo "ctf data file $datafile does not exist"
    exit 4
  fi
  cat $datafile | awk '
/# Pixel size: / { apix=$4; next } \
!/# / { defocus_1=$2; defocus_2=$3; astig=$4; phase_shift=$5; cross_correlation=$6; resolution=$7; next } \
END { \
  resolution_performance= 2 * apix / resolution;
  print "declare -A ctf; ctf[apix]="apix " ctf[defocus_1]="defocus_1 " ctf[defocus_2]="defocus_2 " ctf[astigmatism]="astig " ctf[phase_shift]="phase_shift " ctf[cross_correlation]="cross_correlation " ctf[resolution]="resolution " ctf[resolution_performance]="resolution_performance;
}'
}

motioncor_file()
{
  local input=$1
  local extension="${input##*.}"
  local basename=$(basename -- "$input")
  # sometimes it has the extention?!
  local datafile="${input}.log0-Patch-Full.log"
  if [[ $basename == FoilHole_* ]]; then
    datafile="${input%.${extension}}.log0-Patch-Full.log"
  fi
  echo $datafile
}

parse_motioncor()
{
  local input=$1
  local datafile=$(motioncor_file "$input") 
  if [ ! -e $datafile ]; then
    >&2 echo "motioncor2 data file $datafile does not exist"
    exit 4
  fi
  cat $datafile | grep -vE '^$' | awk '
!/# / { 
  if( $1 > 1 ){ 
    x=$2; y=$3; 
    dx=lastx-x; dy=lasty-y; 
    n=sqrt((dx*dx)+(dy*dy)); 
    drifts[$1-1]=n; 
} lastx=$2; lasty=$3; next; } 
END { 
  for (i = 1; i <= length(drifts); ++i) {
    if( i <= 3 ){ first3 += drifts[i] }
    if( i <= 5 ){ first5 += drifts[i] }
    if( i <= 8 ){ first8 += drifts[i] }
    all += drifts[i]
  }
  print "declare -A align; align[first1]="drifts[1] " align[first3]="first3/3 " align[first5]="first5/5 " align[first8]="first8/8 " align[all]="all/length(drifts) " align[frames]="length(drifts)+1;
}'
# print "    - "lastx "-"x" ("dx*dx")\t" lasty "-"y" ("dy*dy"):\t" n;

}

create_motioncor_star()
{
  local input=$1
  local datafile=$(motioncor_file "$input") 
  if [ ! -e $datafile ]; then
    >&2 echo "motioncor2 data file $datafile does not exist"
    exit 4
  fi
  local output="${datafile%.log0-Patch-Full.log}.star"
  
  >&2 echo "creating alignment star file $output"

  local binning=1
  if [ $SUPERRES -eq 1 ]; then
    binning=2
  fi

  module load ${IMOD_LOAD}
  local info=$(header ${MICROGRAPH} | grep 'Number of columns,')
  local x=$(echo $info | awk '{print $7}')
  local y=$(echo $info | awk '{print $8}')
  local z=$(echo $info | awk '{print $9}')

  cat <<EOF > $output 

data_general

_rlnImageSizeX                                     ${x}
_rlnImageSizeY                                     ${y}
_rlnImageSizeZ                                       ${z}
_rlnMicrographMovieName                    ${MICROGRAPH}
_rlnMicrographGainName                     ${GAINREF_FILE}
_rlnMicrographBinning                                 ${binning}
_rlnMicrographOriginalPixelSize                    ${APIX}
_rlnMicrographDoseRate                             ${FMDOSE}
_rlnMicrographPreExposure                             ${INITDOSE}
_rlnVoltage                                         ${KV}
_rlnMicrographStartFrame                              1
_rlnMotionModelVersion                                0


data_global_shift

loop_
_rlnMicrographFrameNumber #1
_rlnMicrographShiftX #2
_rlnMicrographShiftY #3  
EOF
  cat $datafile | tail -n +4 >> $output
  echo >> $output
  
  #>&2 cat $output

  echo $output
}


set -e
main "$@"


