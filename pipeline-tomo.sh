#!/bin/bash -e

# module loads for programs
IMOD_VERSION="4.9.12"
IMOD_LOAD="imod/${IMOD_VERSION}"
EMAN2_VERSION="2.31"
#EMAN2_VERSION="20190917"
EMAN2_LOAD="eman2/${EMAN2_VERSION}"
#MOTIONCOR2_VERSION="1.1.0"
#MOTIONCOR2_LOAD="motioncor2-1.1.0-gcc-4.8.5-zhoi3ww"
MOTIONCOR2_VERSION="1.2.3"
MOTIONCOR2_LOAD="motioncor2/${MOTIONCOR2_VERSION}"
CTFFIND4_VERSION="4.1.13"
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
GROUP=${GROUP:-"1 1"}
FMREF=${FMREF:-0}
INITDOSE=${INITDOSE:-0}

# PICKING
PARTICLE_SIZE=${PARTICLE_SIZE:-150}
PARTICLE_SIZE_MIN=${PARTICLE_SIZE_MIN:-$(echo $PARTICLE_SIZE | awk '{ print $1*0.8 }')}
PARTICLE_SIZE_MAX=${PARTICLE_SIZE_MAX:-$(echo $PARTICLE_SIZE | awk '{ print $1*1.2 }')}

# toomogram
TOMOGRAM_TILTSTEP=${TOMOGRAM_TILTSTEP}
TOMOGRAM_TILTAXIS=${TOMOGRAM_TILTAXIS}
MDOC=${MDOC}

PARALLEL=${PARALLEL:-0}

# usage
usage() {
  cat <<__EOF__
Usage: $0 MICROGRAPH_FILE

Mandatory Arguments:
  [-a|--apix FLOAT]            use specified pixel size
  [-d|--fmdose FLOAT]          use specified fmdose in calculations

Optional Arguments:
  [-g|--gainref GAINREF_FILE]  use specificed gain reference file
  [-d|--defect DEFECT_FILE]    use specificed defect reference file
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
      "--defect") set -- "$@" "-d";;
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
      "--mdoc")    set -- "$@" "-c";;
      "--parallel")    set -- "$@" "-l";;
      *)           set -- "$@" "$arg";;
   esac
  done

  while getopts "Fhspm:t:l:g:b:a:d:k:e:c:" opt; do
    case "$opt" in
    g) GAINREF_FILE="$OPTARG";;
    d) DEFECT_FILE="$OPTARG";;
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
    c) MDOC="$OPTARG";;
    l) PARALLEL="$OPTARG";;
    h) usage; exit 0;;
    ?) usage; exit 1;;
    esac
  done

  # read from mdocs
  if [[ "$MODE" == "tomo" && -z $MDOC ]]; then
    echo "Need mdoc filepath [-c|--mdoc] to continue..."
    usage
    exit 1
  fi
  if [ -e "${MDOC}" ]; then
    KV=${KV:-$(get_mdoc_voltage)}
    APIX=${APIX:-$(get_mdoc_apix)}
    FMDOSE=${FMDOSE:-$(get_mdoc_fmdose)}
  fi

  MICROGRAPHS=${@:$OPTIND}
  #>&2 echo "MICROGRAPHS: ${MICROGRAPHS} ${#MICROGRAPHS[@]}"
  if [[ ${#MICROGRAPHS[@]} -lt 1 || "${MICROGRAPHS}" == "" ]]; then
    echo "Need input micrograph MICROGRPAH_FILE to continue..."
    usage
    exit 1
  fi

  if [ -z $APIX ]; then
    echo "Need pixel size [-a|--apix] to continue..."
    usage
    exit 1
  fi
  if [[ "$MODE" == "spa" && -z $FMDOSE && ( "$TASK" == "all" || "$TASK" == "align" || "$TASK" == "sum" ) ]]; then
    echo "Need fmdose [-d|--fmdose] to continue..."
    usage
    exit 1
  fi

  local count=0

  # call oneself with parallel to parallelize computation
  if [[ ${PARALLEL} -gt 1 ]]; then

    tmpfile=$(mktemp /tmp/pipeline-parallel.XXXXXX) || exit $?
    for m in ${MICROGRAPHS}; do
      echo $m >> ${tmpfile}
    done
    local common_path=$(cat ${tmpfile} | xargs -n1 dirname | uniq | sed -e 'N;s/^\(.*\).*\n\1.*$/\1/' )
    #echo "BASENAME: ${common_path##*/}"
    BASENAME="${common_path##*/}"
    local mdoc=''
    if [ ! -z $MDOC ]; then 
      mdoc=" --mdoc \"${MDOC}\" "
    fi
    cat ${tmpfile} | parallel --jobs=${PARALLEL} GPU={%} $0 -m ${MODE} -t ${TASK} --basename ${BASENAME} $mdoc  {} ::::
    >&2 rm -f ${tmpfile} || exit $?

  else

    for MICROGRAPH in ${MICROGRAPHS}; do

      >&2 echo "MICROGRAPH: ${MICROGRAPH}"
      if [ "$MODE" == "spa" ]; then
        do_spa
      elif [ "$MODE" == "tomo" ]; then
        do_tomo
      else
        echo "Unknown MODE $MODE"
        usage
        exit 1
      fi
      count=$((count+1))
    done

  fi

  if [ "$MODE" == "tomo" ]; then
    #echo "NUM: $count: ${MICROGRAPHS}"
    if [[ -z ${BASENAME} && $count -gt 1 ]]; then
      tmpfile=$(mktemp /tmp/pipeline-parallel.XXXXXX) || exit $?
      for m in ${MICROGRAPHS}; do
        echo $m >> ${tmpfile}
      done
      local common_path=$(cat ${tmpfile} | xargs -n1 dirname | uniq | sed -e 'N;s/^\(.*\).*\n\1.*$/\1/' )
      BASENAME="${common_path##*/}"
      >&2 rm -f ${tmpfile} || exit $?
    fi
    do_tomo_create
  fi
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
    PREVIEW_FILE=$(generate_preview) || exit $?
    echo "    files:"
    dump_file_meta "${PREVIEW_FILE}" || exit $?
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  fi

}

do_tomo()
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
    if [[ "$GAINREF_FILE" != "" ]]; then
      GAINREF_FILE=$(process_gainref "$GAINREF_FILE") || exit $?
    fi
    FORCE=$force
  fi

  # start doing something!
  echo "tomographic_analysis:"

  if [[ "$TASK" == "align" || "$TASK" == "all" ]]; then
    do_tomo_align
  fi
  if [[ "$TASK" == "sum" || "$TASK" == "all" ]]; then
    do_spa_sum "1"
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
      SUMMED_CTF_FILE=$(sum_ctf_file "${MICROGRAPH}") || exit $?
      # remove the _sum bit if SUMMED_FILE defined
      if [ ! -z $SUMMED_FILE ]; then
        SUMMED_CTF_FILE="${SUMMED_CTF_FILE%_sum_ctf.mrc}_ctf.mrc"
        #>&2 echo "SUMMED CTF: $SUMMED_CTF_FILE"
      fi
      #echo "SUMMED_CTF_FILE: $SUMMED_CTF_FILE"
    fi
    PREVIEW_FILE=$(generate_tomo_preview) || exit $?
    echo "    files:"
    dump_file_meta "${PREVIEW_FILE}" || exit $?
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  fi

}

do_tomo_create()
{
  if [[ "$TASK" == "create" || "$TASK" == "all" ]]; then
    # create non-dw tomogram
    do_tomo_stack "aligned/$BASENAME/motioncor2/$MOTIONCOR2_VERSION/*_aligned.mrc" "tiltstack/$BASENAME/$BASENAME.st"
    do_tomo_eman2
  fi
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
  if [[ "$MODE" == 'spa' ]] ; then
    echo "  - task: micrograph"
    echo "    files:"
    dump_file_meta "${MICROGRAPH}" || exit $?
  fi
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
  file=$(motioncor_file ${ALIGNED_FILE}) || exit $?
  if [ ! -e $file ]; then
    >&2 echo "motioncor2 data file $file does not exist"
    exit 4
  fi
  echo "    source: $file"
  echo "    data:"
  align=$(parse_motioncor ${ALIGNED_FILE}) || exit $?
  eval $align || exit $?
  for k in "${!align[@]}"; do
  if [ "$k" == 'frames' ]; then
    echo "      $k: ${align[$k]}"
  else
    printf "      $k: %.2f\n" ${align[$k]}
  fi
  done

  PROCESSED_ALIGN_FIRST1=${align[first1]}
  PROCESSED_ALIGN_FIRST3=${align[first3]}
  PROCESSED_ALIGN_FIRST5=${align[first5]}
  PROCESSED_ALIGN_FIRST8=${align[first8]}
  PROCESSED_ALIGN_ALL=${align[all]}

  # create a file that relion can read
  star=$(create_motioncor_star ${ALIGNED_FILE}) || exit $?

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
  ctf_file=$(ctffind_file $ALIGNED_CTF_FILE) || exit $?
  echo "    source: $ctf_file"
  ctf_data=$(parse_ctffind $ALIGNED_CTF_FILE) || exit $?
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

get_mdoc_initDose() {
  local file=$1
  local mdoc=${2:-$MDOC}
  cat $mdoc | awk -v file=$file '
/PriorRecordDose/ { dose=$3; } \
/SubFramePath/ { if( match($3,file) ){ print dose; exit 0; } }'
}

get_mdoc_tiltAxisAngle() {
  local mdoc=${1:-$MDOC}
  awk '/ Tilt axis angle = / { angle=$7; gsub(",","",angle); print angle }' $mdoc
}

get_mdoc_apix() {
  local mdoc=${1:-$MDOC}
  awk '/PixelSpacing = / { print $3; exit }' $mdoc 
}

get_mdoc_voltage() {
  local mdoc=${1:-$MDOC}
  awk '/Voltage = / { print $3 }' $mdoc
}

get_mdoc_fmdose() {
  local mdoc=${1:-$MDOC}
  awk '/FrameDosesAndNumber = / { print $3; exit }' $mdoc 
}

do_tomo_align() {

  >&2 echo
  >&2 echo "Processing align for micrograph $MICROGRAPH..."

  echo "  - task: align_stack"
  local start=$(date +%s.%N)
  local working_dir="aligned/${BASENAME}/motioncor2/$MOTIONCOR2_VERSION"
  local file=$(basename $MICROGRAPH)
  INITDOSE=$(get_mdoc_initDose "$file") || exit $?
  ALIGNED_FILE=$(align_stack "$MICROGRAPH" "$GAINREF_FILE" "$working_dir") || exit $?
  >&2 rm -f $tmpfile || exit 4
  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  echo "    duration: $duration"
  echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  echo "    files:"
  dump_file_meta "${ALIGNED_FILE}" || exit $?
  ALIGNED_DW_FILE="${ALIGNED_FILE%.mrc}_DW.mrc"
  dump_file_meta "${ALIGNED_DW_FILE}" || exit $?

  echo "  - task: align_data"
  file=$(motioncor_file ${ALIGNED_FILE}) || exit $?
  echo "    source: $file"
  echo "    data:"
  align=$(parse_motioncor ${ALIGNED_FILE}) || exit $?
  eval $align || exit $?
  for k in "${!align[@]}"; do
  echo "      $k: ${align[$k]}"
  done

  PROCESSED_ALIGN_FIRST1=${align[first1]}
  PROCESSED_ALIGN_FIRST3=${align[first3]}
  PROCESSED_ALIGN_FIRST5=${align[first5]}
  PROCESSED_ALIGN_FIRST8=${align[first8]}
  PROCESSED_ALIGN_ALL=${align[all]}

  ###
  # use ctffid
  ###
  echo "  - task: ctf_align"
  local start=$(date +%s.%N)
  # we always bin down the aligned file if superres, so we need to prevent the ctf from using the wrong apix value
  local orig_superres=${SUPERRES}
  SUPERRES=0
  local outdir="aligned/${BASENAME}/motioncor2/$MOTIONCOR2_VERSION/ctffind4/$CTFFIND4_VERSION"
  ALIGNED_CTF_FILE=$(process_ctffind "$ALIGNED_FILE" "$outdir") || exit $?
  SUPERRES=${orig_superres}
  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  echo "    duration: $duration"
  echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  echo "    files:"
  dump_file_meta  "${ALIGNED_CTF_FILE}" || exit $?

  echo "  - task: ctf_align_data"
  ctf_file=$(ctffind_file $ALIGNED_CTF_FILE) || exit $?
  echo "    source: $ctf_file"
  ctf_data=$(parse_ctffind $ALIGNED_CTF_FILE) || exit $?
  eval $ctf_data || exit $?
  echo "    data:"
  for k in "${!ctf[@]}"; do
  echo "      $k: ${ctf[$k]}"
  done

  PROCESSED_ALIGN_RESOLUTION=${ctf[resolution]}
  PROCESSED_ALIGN_RESOLUTION_PERFORMANCE=${ctf[resolution_performance]}
  PROCESSED_ALIGN_ASTIGMATISM=${ctf[astigmatism]}
  PROCESSED_ALIGN_CROSS_CORRELATION=${ctf[cross_correlation]}

  #echo "  - task: ctf_align"
  #local start=$(date +%s.%N)
  ## we always bin down the aligned file if superres, so we need to prevent the ctf from using the wrong apix value
  #local orig_superres=${SUPERRES}
  #SUPERRES=0
  #local outdir="aligned/${BASENAME}/motioncor2/$MOTIONCOR2_VERSION/eman2/$EMAN2_VERSION"
  #ALIGNED_CTF_FILE=$(process_tomoctf "$ALIGNED_FILE" "$outdir") || exit $?
  #SUPERRES=${orig_superres}
  #local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  #echo "    duration: $duration"
  #echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  #echo "    files:"
  #dump_file_meta  "${ALIGNED_CTF_FILE}" || exit $?

  #echo "  - task: ctf_align_data"
  #local ctf_file=$(ctffind_file $ALIGNED_CTF_FILE) || exit $?
  #echo "    source: $ctf_file"
  #local ctf_data=$(parse_ctffind $ALIGNED_CTF_FILE) || exit $?
  #eval $ctf_data || exit $?
  #echo "    data:"
  #for k in "${!ctf[@]}"; do
  #echo "      $k: ${ctf[$k]}"
  #done

  #PROCESSED_ALIGN_RESOLUTION=${ctf[resolution]}
  #PROCESSED_ALIGN_RESOLUTION_PERFORMANCE=${ctf[resolution_performance]}
  #PROCESSED_ALIGN_ASTIGMATISM=${ctf[astigmatism]}
  #PROCESSED_ALIGN_CROSS_CORRELATION=${ctf[cross_correlation]}

  #
}

do_tomo_stack()
{
  local dir="${MICROGRAPH}"
  >&2 echo "micrograph for stacking: ${MICROGRAPH}"
  if [ ! -d "$dir" ]; then
    dir="$(dirname ${MICROGRAPH})"
  fi
  local input_glob=${1:-aligned/$BASENAME/motioncor2/$MOTIONCOR2_VERSION/*_aligned.mrc}
  local output=${2:-tiltstack/$BASENAME/$BASENAME.st}

  local out_rawtlt=${output%.st}.rawtlt
  if [ -e $out_rawtlt ]; then
    >&2 echo "raw tilt file $out_rawtlt already exists..."
  fi

  >&2 echo "stacking tilts from $dir with $input_glob"
  echo "  - task: create_rawtlt"
  local start=$(date +%s.%N)
  
  #>&2 module load ${EMAN2_LOAD} || exit 2
  #>&2 e2spt_tiltstacker.py --stem2stack $BASENAME --negativetiltseries --tiltstep $TOMO_STEP --apix $APIX --bidirectional --tiltrange $TOMO_HIGH_ANGLE --anglesindxinfilename $TOMO_INDEX

  local regex='s/.*\_\([0-9]\{5\}\)_\(-\?[0-9]\+\.[0-9]\+\).*/\2 \1 \0/'

  local data=()
  local total_tilts=0
  local duplicate_tilts=0
  local last_angle=''
  IFS=$'\n'
  tmpfile=$(mktemp /tmp/pipeline-tomo_data.XXXXXX) || exit $?
  for line in $(ls $input_glob | sed 's/.*\_\([0-9]\{5\}\)_\(-\?[0-9]\+\.[0-9]\+\).*/\2 \1 \0/' | sort -k1g); do
    #>&2 echo $line
    local angle=$(echo $line | awk '{print $1}')
    >&2 echo "$line" >> $tmpfile
    let "total_tilts=total_tilts+1"
    if [ "$last_angle" == "$angle" ]; then
      let "duplicate_tilts=duplicate_tilts+1"
      #>&2 echo "  duplicate $duplicate_tilts"
    fi
    TOMOGRAM_TILTSTEP=$( echo "$angle $last_angle" | awk '{print $1-$2}' )
    #echo $TOMOGRAM_TILTSTEP
    last_angle="$angle"
  done
  
  # write the list of images to stack
  local num_tilts=$((total_tilts-duplicate_tilts))
  # remove duplicate angls and put a zero after each line
  echo "$num_tilts" > $tmpfile.stack
  tac $tmpfile | sort -buk1,1g | sed -e 's/.* .* \(.*\)$/\1\n0/' >> $tmpfile.stack
  #cat $tmpfile.stack
     
  # write rawtlt file
  >&2 mkdir -p $(dirname ${out_rawtlt})
  tac $tmpfile | sort -buk1,1g | cut -f 1 -d' ' > ${out_rawtlt}
  TOMOGRAM_RAWTLT=${out_rawtlt}

  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  echo "    tilt_step: ${TOMOGRAM_TILTSTEP}"
  echo "    duration: $duration"
  echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  echo "    files:"
  dump_file_meta "$out_rawtlt" || exit $?

  if [ -e $output ]; then
    >&2 echo "output stack file $output already exists"
  fi
  if [[ $FORCE -eq 1 || ! -e $output ]]; then

    >&2 echo "generating stack from micrographs in dir $dir to $output with $num_tilts ($duplicate_tilts duplicates)..."
    echo "  - task: create_stack"
    local start=$(date +%s.%N)

    # create teh stack
    >&2 module load ${IMOD_LOAD}
    stackfile=$(mktemp /tmp/pipeline-stack.XXXXXX) || exit $?
    >&2 echo "executing: newstack -fileinlist $tmpfile.stack $stackfile"
    >&2 newstack -fileinlist $tmpfile.stack $stackfile
    >&2 mv -f $stackfile $output
    >&2 rm -f $tmpfile $tmpfile.stack

    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
    echo "    files:"
    dump_file_meta "$output" || exit $?

  fi
  TOMOGRAM_STACK="$output"
  #echo $output
}

do_tomo_batchruntomo()
{
  local outdir=${2:-.}

  >&2 echo

  if [ ! -d "${MICROGRAPH}" ]; then
    >&2 echo "input $MICOGRAPH must be directoory of tilts"
    exit 4
  fi
  tmpfile=$(mktemp /tmp/pipeline-batchruntomo.XXXXXX) || exit $?
  cat <<EOF > $tmpfile
setupset.copyarg.name=BBa setupset.datasetDirectory=${MICROGRAPH} setupset.copyarg.dual=0 setupset.copyarg.pixel=${APIX} setupset.copyarg.gold=10 setupset.copyarg.rotation=-12.5 setupset.copyarg.extract=1 runtime.Preprocessing.any.removeXrays=1 runtime.Fiducials.any.trackingMethod=0 runtime.Fiducials.any.seedingMethod=1 comparam.track.beadtrack.LightBeads=0 comparam.track.beadtrack.LocalAreaTracking=1 comparam.track.beadtrack.SobelFilterCentering=1 comparam.track.beadtrack.KernelSigmaForSobel=1.5 runtime.BeadTracking.any.numberOfRuns=2 comparam.autofidseed.autofidseed.TargetNumberOfBeads=8 comparam.autofidseed.autofidseed.AdjustSizes=1 comparam.autofidseed.autofidseed.TwoSurfaces=0 comparam.align.tiltalign.SurfacesToAnalyze=1 comparam.align.tiltalign.LocalAlignments=1 comparam.align.tiltalign.MagOption=3 comparam.align.tiltalign.TiltOption=5 comparam.align.tiltalign.RotOption=3 comparam.align.tiltalign.RobustFitting=1 runtime.AlignedStack.any.linearInterpolation=1 runtime.AlignedStack.any.binByFactor=1 runtime.Reconstruction.any.useSirt=0 runtime.Reconstruction.any.doBackprojAlso=1 comparam.tilt.tilt.LOG= runtime.Trimvol.any.reorient=1 comparam.tilt.tilt.THICKNESS=700
EOF
  >&2 cat $tmpfile
  >&2 module load ${IMOD_LOAD} || exit 2
  >&2 batchruntomo -directive $tmpfile
  rm -f $tmpfile

}

do_tomo_eman2()
{
  local instack=${1:-$TOMOGRAM_STACK}
  local tiltstep=${2:-$TOMOGRAM_TILTSTEP}
  local rawtlt=${3:-$TOMOGRAM_RAWTLT}
  local tiltaxis=${4:-$TOMOGRAM_TILTAXIS}

  local niter=${TOMOGRAM_NITER:-"3,2,1,1"}

  local outdir=${5:-/tmp/}

  if [ ! -e $instack ]; then
    >&2 echo "input stack file $input doesn't exist!"
  fi
  if [ "$tiltaxis" == '' ]; then
    tiltaxis=$(get_mdoc_tiltAxisAngle "${MDOC}")
  fi
  #tiltaxis='-3.0'

  local dir=$(dirname -- $instack)
  >&2 module load ${EMAN2_LOAD}

  # setup eman2
  local instack_file=$(basename -- "$instack")
  local extension="${instack_file##*.}"
  local basename=${instack_file%.$extension}

  local output_file=$basename"__bin4.hdf"
  local output="./tomograms/"$basename"/"$output_file
  mkdir -p $(dirname $output)
  if [ -e $output ]; then
    >&2 echo "tomogram $output already exists..."
  fi
  if [[ $FORCE -eq 1 || ! -e $output ]]; then
  
    local tmpdir=$(mktemp -d /tmp/tomo-create.XXXX )
    local workdir=$(pwd)
    >&2 pushd $tmpdir
    # create a tiltseries that eman2 can use (duplication of effort)
    >&2 echo "importing tiltstack from $instack... $basename"
    >&2 echo "executing: e2import.py $workdir/$instack --apix=$APIX --import_tiltseries --importation=copy"
    echo "  - task: import_stack"
    local start=$(date +%s.%N)
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
    >&2 e2import.py $workdir/$instack --apix=$APIX --import_tiltseries --importation=copy
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"

    local tiltseries="./tiltseries/$BASENAME.hdf"

    # do tomo
    >&2 echo "creating tomogram $output from $rawtlt with $tiltstep degree step and $tiltaxis axis angle..."
    >&2 echo "executing: e2tomogram.py ${tiltseries} --tltstep=${tiltstep} --rawtlt=$workdir/${rawtlt} --npk=20 --tltax=${tiltaxis} --tltkeep=0.9 --outsize=1k --niter=${niter} --bytile --pkkeep=0.9 --clipz=-1 --bxsz=32 --pk_mindist=0.125 --correctrot --normslice --filterto=0.45 --extrapad --threads=${SLURM_CPUS_ON_NODE:-2} --ppid=-2 --rmbeadthr=5"
    #>&2 echo "e2tomogram.py ${tiltseries} --tltstep=${tiltstep} --rawtlt=$workdir/${rawtlt} --npk=20 --tltkeep=0.9 --outsize=1k --niter=2,1,1,1 --bytile --pkkeep=0.9 --clipz=-1 --bxsz=32 --pk_mindist=0.125 --correctrot --normslice --filterto=0.45 --extrapad --threads=${SLURM_CPUS_ON_NODE} --ppid=-2 --rmbeadthr=5"
    echo "  - task: create_tomogram"
    local start=$(date +%s.%N)
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
    >&2 e2tomogram.py ${tiltseries} --tltstep=${tiltstep} --rawtlt=$workdir/${rawtlt} --npk=20  --tltax=${tiltaxis} --tltkeep=0.9 --outsize=1k --niter=${niter} --bytile --pkkeep=0.9 --clipz=-1 --bxsz=32 --pk_mindist=0.125 --correctrot --normslice --filterto=0.45 --extrapad --threads=${SLURM_CPUS_ON_NODE:-2} --ppid=-2 --rmbeadthr=5 --notmp
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    >&2 popd
    local destdir="./tomograms/"$basename
    >&2 mkdir -p $destdir/
    >&2 mv -f $tmpdir/tomograms/$output_file $destdir/ || exit $?
    # clean up
    >&2 rm -rf $tmpdir
    dump_file_meta "${output}" || exit $?

  fi

  TOMOGRAM_NONDW=$output

}

process_tomoctf()
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

  if [[ $FORCE -eq 1 || ! -e $output || ! -s $output ]]; then
    >&2 echo "ctf'ing micrograph $input to $output..."
    echo "  - task: ctf_INPUT"
    local start=$(date +%s.%N)
    module load ${EMAN2_LOAD} || exit $?
    echo "Executing: e2tomo_ctfraw.py --input ${input} --tiltangle ${TOMOGRAM_TILTAXIS} --cs ${CS} --voltage ${KV}"
    >&2 e2tomo_ctfraw.py --input ${input} --tiltangle ${TOMOGRAM_TILTAXIS} --cs ${CS} --voltage ${KV} || exit $?
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  fi

  if [ $? -ne 0 ]; then
    exit 1
  fi

  echo $output

}

sum_ctf_file()
{
  local input="$1"
  if [[ ! -z "${BASENAME}" && "$MODE" != 'tomo' ]]; then
    input="${BASENAME}"
  fi
  local extension="${input##*.}"
  local outdir=${2:-summed/imod/$IMOD_VERSION/ctffind4/$CTFFIND4_VERSION}
  local output="$outdir/${input%.${extension}}_sum_ctf.mrc"
  # put basename into filepath if tomo
  if [ "${MODE}" == "tomo" ]; then
    local basename=$(basename -- $input)
    output="${outdir/summed/summed/${BASENAME}}/${basename%.${extension}}_sum_ctf.mrc"
  fi
  echo $output
}

align_ctf_file()
{
  local input="$1"
  if [[ ! -z "${BASENAME}" && "$MODE" != 'tomo' ]]; then
    input="${BASENAME}"
  fi
  local extension="${input##*.}"
  local outdir=${2:-aligned/motioncor2/$MOTIONCOR2_VERSION/ctffind4/$CTFFIND4_VERSION}
  local output="$outdir/${input%.${extension}}_aligned_ctf.mrc"
  # put basename into filepath if tomo
  if [ "${MODE}" == "tomo" ]; then
    local basename=$(basename -- $input)
    output="${outdir/aligned/aligned/${BASENAME}}/${basename%.${extension}}_aligned_ctf.mrc"
  fi
  echo $output
}

###
# create the ctf and preview images for the summed stack of the MICROGRAPH. creating the sum temporarily if necessary
###
do_spa_sum() {

  >&2 echo
  >&2 echo "Processing sum for micrograph $MICROGRAPH..."

  local generate_sum_preview=$1
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
  if [[ -z $SUMMED_FILE && ( $FORCE -eq 1 || ! -e $SUMMED_CTF_FILE || ! -s $SUMMED_CTF_FILE ) ]]; then
    echo "  - task: sum"
    local start=$(date +%s.%N)
    file=$(basename -- "$SUMMED_CTF_FILE") || exit $?
    local tmpfile="/tmp/${file%_ctf.mrc}.mrc"
    SUMMED_FILE=$(process_sum "$MICROGRAPH" "$tmpfile" "$GAINREF_FILE") || exit $?
    summed_file_log="${SUMMED_FILE%.mrc}.log"
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  fi

  echo "  - task: ctf_summed"
  local start=$(date +%s.%N)
  if [[ $FORCE -eq 1 || ! -e "$SUMMED_CTF_FILE" || ! -s $SUMMED_CTF_FILE ]]; then
    outdir=$(dirname "$SUMMED_CTF_FILE") || exit $?
    SUMMED_CTF_FILE=$(process_ctffind "$SUMMED_FILE" "$outdir") || exit $?
    if [ "$generate_sum_preview" == "1" ]; then
      SUMMED_PREVIEW=$(generate_image $SUMMED_FILE "previews" 0.05)
    fi
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
  ctf_file=$(ctffind_file $SUMMED_CTF_FILE) || exit $?
  echo "    source: $ctf_file"
  ctf_data=$(parse_ctffind $SUMMED_CTF_FILE) || exit $?
  eval $ctf_data
  echo "    data:"
  for k in "${!ctf[@]}"; do
  echo "      $k: ${ctf[$k]}"
  done

  PROCESSED_SUM_RESOLUTION=${ctf[resolution]}
  PROCESSED_SUM_RESOLUTION_PERFORMANCE=${ctf[resolution_performance]}

}

summed_preview_file()
{
  local input=$(basename -- $1)
  local dirname=${2:-previews}
  local extension="${input##*.}"
  local output="$dirname/${input%.${extension}}_sum.jpg"
  echo $output
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
  particles=$(wc -l ${PARTICLE_FILE} | awk '{print $1-11}') || exit $?
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
  if [[ ${input:0:2} == './' ]]; then input=${input#./}; fi

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
      # assume $input is always superres, so scale down if not
      dm2mrc "$input" "$output"  1>&2 || exit $?
      if [[ "$SUPERRES" == "0" ]]; then
        >&2 echo "binning gain ref for non-superres $SUPERRES"
        >&2 newstack -bin 2 "$output" "/tmp/${filename%.$extension}.mrc" && mv "/tmp/${filename%.$extension}.mrc" "$output" || exit $?
      fi
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
  if [[ ! -z "${BASENAME}" && ! "${MODE}" == "tomo" ]]; then
    >&2 echo "setting input to $BASENAME"
    input="${BASENAME}"
  fi
  local filename=$(basename -- "$input")
  local outdir=${2:-aligned/motioncor2/$MOTIONCOR2_VERSION}
  local extension="${filename##*.}"
  local output="$outdir/${filename%.${extension}}_aligned.mrc"

  # ensure we keep the file details for previews tomo (ignored if all tasks)
  if [[ "${MODE}" == "tomo" && "${TASK}" == "preview" ]]; then
    output="${outdir/aligned/aligned/${BASENAME}}/${filename%.${extension}}_aligned.mrc"
  fi
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
  output=$(align_file $input $outdir) || exit $?

  if [ -e $output ]; then
    >&2 echo "aligned file $output already exists"
  fi
  
  if [[ $FORCE -eq 1 || ! -e $output ]]; then

    # deal with when motioncor2 thinks the mrc is bad (half width dimension)
    if [[ "$MODE" == "tomo" && ! -z ${GAINREF_FILE} && ! -z ${DEFECT_FILE} ]]; then
      >&2 echo "creating temporary movie to correct for frame sizes..."
      >&2 module load ${IMOD_LOAD}
      local tmpfile=$(basename -- $MICROGRAPH.mrc)
      tmpfile="/tmp/$tmpfile"
      #>&2 echo executing: e2proc2d.py $MICROGRAPH $tmpfile
      #>&2 e2proc2d.py $MICROGRAPH $tmpfile || exit 4
      local defect=" -D \"${DEFECT_FILE}\" "
      >&2 echo "excuting: clip norm $defect -R -1 -m 1 \"${MICROGRAPH}\" \"${GAINREF_FILE}\"  $tmpfile"
      >&2 clip norm $defect -R -1 -m 1 "${MICROGRAPH}" "${GAINREF_FILE}"  $tmpfile || exit $?
      input="$tmpfile"
    fi

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
        -Group $GROUP \
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
    >&2 eval $align_command  || echo $?
  fi

  if [ "$MODE" == "tomo" ]; then
    rm -f "$tmpfile"
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

  if [[ $FORCE -eq 1 || ! -e $output || ! -s $output ]]; then
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

generate_image()
{
  local input=$1
  local outdir=${2:-.}
  local lowpass=${3:-}
  local format=${4:-jpg}

  >&2 echo

  if [ ! -e $input ]; then
    >&2 echo "input micrograph $input not found"
    exit
  fi

  local filename=$(basename -- "$input")
  local extension="${filename##*.}"
  local output="$outdir/${filename%.${extension}}.${format}"
  mkdir -p $outdir

  if [ -e $output ]; then
    >&2 echo "preview file $output already exists"
  fi

  if [[ $FORCE -eq 1 || ! -e $output ]]; then
    >&2 rm -f $output
    >&2 echo "generating preview of $input to $output..."
    module load ${IMOD_LOAD} || exit $?
    tmpfile=$input
    if [ "$lowpass" != "" ]; then
      tmpfile=$(mktemp /tmp/pipeline-image.XXXXXX)
      >&2 echo "executing: clip filter -l $lowpass $input $tmpfile" 1>&2
      >&2 clip filter -l $lowpass $input $tmpfile || exit $?
      if [ ! -e $tmpfile ]; then
        >&2 echo "could not create image $tmpfile... exiting..."
        exit 4
      fi
    fi
    local opts=""
    if [ "${format}" == "jpg" ]; then
      opts="-j"
    fi
    echo "executing: mrc2tif ${opts} $tmpfile $output" 1>&2
    >&2 mrc2tif ${opts} $tmpfile $output || exit $?
    if [ "$lowpass" != "" ]; then
      >&2 echo "rm -f $tmpfile"
      rm -f $tmpfile || exit $?
    fi
  fi

  if [ ! -e $output ]; then
    >&2 echo "could not generate preview file $output!"
    exit 4
  fi

  >&2 echo "done"
  echo $output
}

process_sum()
{
  local input=$1
  local output=$2
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
    tmpfile=$(mktemp /tmp/pipeline-sum.XXXXXX) || exit $?
    module load ${IMOD_LOAD} || exit $?
    >&2 echo "avgstack $input $tmpfile /"
    avgstack > $log << __AVGSTACK_EOF__
$input
$tmpfile
/
__AVGSTACK_EOF__
    module load ${IMOD_LOAD} || exit $?
    if [[ "$gainref" != "" ]]; then
      >&2 echo clip mult -n 16 $tmpfile \'$gainref\' \'$output\'
      clip mult -n 16 $tmpfile "$gainref" "$output"  1>&2 || exit $?
      rm -f $tmpfile || exit $?
    else
      mv -f $tmpfile $output
    fi
  fi
  if [ ! -e $output ]; then
    >&2 echo "could not generate sum file $output!"
    exit 4
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

  output=$(particle_file "$input") || exit $?

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
  stat=$(stat -c "%s/%y/%w" "$file") || exit $?
  mod=$(date --utc -d "$(echo $stat | cut -d '/' -f 2)"  +%FT%TZ) || exit $?
  create=$(echo $stat | cut -d '/' -f 3) || exit $?
  if [ "$create" == "-" ]; then create=$mod; fi
  size=$(echo $stat | cut -d '/' -f 1) || exit $?
  echo "file=\"$1\" checksum=$md5 size=$size modify_timestamp=$mod create_timestamp=$create"
}

dump_file_meta()
{
  if [ ! -e "$1" ]; then
    >&2 echo "File '$1' does not exist."
    exit 4
  fi
  echo "      - path: $1"
  out=$(generate_file_meta "$1") || exit $?
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
  filename=$(basename -- "$MICROGRAPH") || exit $?
  if [ ! -z ${BASENAME} ]; then
    filename="${BASENAME}_sidebyside.jpg"
  fi
  local extension="${filename##*.}"
  local output="$outdir/${filename}"

  # create a preview of the image
  # create the picked preview
  #local picked_preview=/tmp/tst.jpg
  picked_preview=$(mktemp /tmp/pipeline-picked-XXXXXXXX.jpg) || exit $?

  if [ -e "$picked_preview" ]; then
    >&2 echo "particle picked preview file $picked_preview already exists..."
  fi

  if [ ! -e "$ALIGNED_DW_FILE" ]; then
    >&2 echo "aligned file $ALIGNED_DW_FILE not found..."
    exit 4
  fi
  aligned_jpg=$(generate_jpg "$ALIGNED_DW_FILE" /tmp 0.05) || exit $?
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
    size=$( echo "$PARTICLE_SIZE * $APIX" | awk '{ print $1 }' ) || exit $?
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
  timestamp=$(TZ=America/Los_Angeles date +"%Y-%m-%d %H:%M:%S" -r ${MICROGRAPH}) || exit $?

  # create the top half
  if [ ! -e "$SUMMED_CTF_FILE" ]; then
    >&2 echo "summed ctf file $SUMMED_CTF_FILE not found..."
    exit 4
  fi
  SUMMED_CTF_PREVIEW=$(generate_jpg "${SUMMED_CTF_FILE}" "/tmp" ) || exit $?

  local top=$(mktemp /tmp/pipeline-top-XXXXXXXX.jpg)
  res="$(printf '%.1f' ${PROCESSED_SUM_RESOLUTION:-0.0})Å ($(echo ${PROCESSED_SUM_RESOLUTION_PERFORMANCE:-0.0} | awk '{printf( "%2.0f", $1*100 )}')%)" || exit $?
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
  bottom=$(mktemp /tmp/pipeline-bottom-XXXXXXXX.jpg) || exit $?
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

  if [ ! -e $output ]; then
    exit 4
  fi

  echo $output
}

generate_tomo_preview()
{
  local outdir=${1:-previews}
  mkdir -p $outdir
  filename=$(basename -- "$MICROGRAPH") || exit $?
  local extension="${filename##*.}"
  local output="$outdir/${filename%.${extension}}.jpg"

  ALIGNED_DW_FILE=$(align_dw_file "$MICROGRAPH") || exit $?
  if [ ! -e "$ALIGNED_DW_FILE" ]; then
    >&2 echo "aligned file $ALIGNED_DW_FILE not found..."
    exit 4
  fi
  aligned_jpg=$(generate_image "$ALIGNED_DW_FILE" /tmp 0.05) || exit $?

  module load ${IMAGEMAGICK_LOAD} || exit $?

  # get a timestamp of when file was created
  timestamp=$(TZ=America/Los_Angeles date +"%Y-%m-%d %H:%M:%S" -r ${MICROGRAPH}) || exit $?

  # create the top half
  if [ ! -e "$SUMMED_CTF_FILE" ]; then
    >&2 echo "summed ctf file $SUMMED_CTF_FILE not found..."
    exit 4
  fi
  SUMMED_CTF_PREVIEW=$(generate_image  "${SUMMED_CTF_FILE}" "/tmp" "") || exit $?

  local top=$(mktemp /tmp/pipeline-top-XXXXXXXX.jpg)
  res="$(printf '%.1f' ${PROCESSED_SUM_RESOLUTION:-0.0})Å ($(echo ${PROCESSED_SUM_RESOLUTION_PERFORMANCE:-0.0} | awk '{printf( "%2.0f", $1*100 )}')%)" || exit $?
  #>&2 echo "ctf res $res"
  local summed_jpg=$(summed_preview_file ${MICROGRAPH})
  convert \
    -resize '512x512^' -extent '512x512' -flip $summed_jpg \
    -flip ${SUMMED_CTF_PREVIEW} \
    +append -font DejaVu-Sans -pointsize 24 -fill yellow -stroke orange -strokewidth 1 -draw "text 529,502 \"${timestamp}\"" \
    +append -font DejaVu-Sans -pointsize 24 -fill yellow -stroke orange -strokewidth 1 -draw "text 864,502 \"$res\"" \
    $top \
  || exit $?

  rm -f $summed_jpg $SUMMED_CTF_PREVIEW || exit $?

  # create the bottom half
  if [ ! -e "$ALIGNED_CTF_FILE" ]; then
    >&2 echo "aligned ctf file $ALIGNED_CTF_FILE not found..."
    exit 4
  fi
  ALIGNED_CTF_PREVIEW=$(generate_image "${ALIGNED_CTF_FILE}" "/tmp" "" "tif") || exit $?
  bottom=$(mktemp /tmp/pipeline-bottom-XXXXXXXX.jpg) || exit $?
  local res="$(printf '%.1f' ${PROCESSED_ALIGN_RESOLUTION:-0.0})Å ($(echo ${PROCESSED_ALIGN_RESOLUTION_PERFORMANCE:-0.0} | awk '{printf( "%2.0f", $1*100)}')%)"
  local ctf="cs $(printf '%.2f' ${PROCESSED_ALIGN_ASTIGMATISM:-0.0}) cc $(printf '%.2f' ${PROCESSED_ALIGN_CROSS_CORRELATION:-0.0})"
  local drift="$(printf "%.2f" ${PROCESSED_ALIGN_FIRST1:-0.0}) "/" $(printf "%.2f" ${PROCESSED_ALIGN_FIRST5:-0.0}) "/" $(printf "%.2f" ${PROCESSED_ALIGN_ALL:-0.0})"
 # >&2 echo "RES: $res DRIFT: $drift"
  convert \
    -resize '512x512^' -extent '512x512' \
    $aligned_jpg \
    ${ALIGNED_CTF_PREVIEW} \
    +append -font DejaVu-Sans -pointsize 24 -fill orange -stroke orange2 -strokewidth 1 -draw "text 274,30 \"$drift\"" \
    +append -font DejaVu-Sans -pointsize 24 -fill orange -stroke orange2 -strokewidth 1 -draw "text 529,30 \"$ctf\"" \
    +append -font DejaVu-Sans -pointsize 24 -fill orange -stroke orange2 -strokewidth 1 -draw "text 864,30 \"$res\"" \
    $bottom \
 || exit $?

  # clean files
  rm -f $aligned_jpg $ALIGNED_CTF_PREVIEW || exit $?

  convert $top $bottom \
     -append $output \
  || exit $?
  rm -f $top $bottom  || exit $?

  if [ ! -e $output ]; then
    exit 4
  fi

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
  datafile=$(ctffind_file "$input") || exit $?

  if [ ! -e $datafile ]; then
    >&2 echo "ctf data file $datafile does not exist"
    exit 4
  fi
  >&2 echo "parsing ctf data from $datafile..."
  cat $datafile | awk '
/# Pixel size: / { apix=$4; next } \
!/# / { defocus_1=$2; defocus_2=$3; astig=$4; phase_shift=$5; cross_correlation=$6; resolution=$7; next } \
END { \
  if (resolution=="inf") {
    print "declare -A ctf; ctf[apix]="apix " ctf[defocus_1]="defocus_1 " ctf[defocus_2]="defocus_2 " ctf[astigmatism]="astig " ctf[phase_shift]="phase_shift " ctf[cross_correlation]="cross_correlation;
  } else {
    resolution_performance= 2 * apix / resolution;
    print "declare -A ctf; ctf[apix]="apix " ctf[defocus_1]="defocus_1 " ctf[defocus_2]="defocus_2 " ctf[astigmatism]="astig " ctf[phase_shift]="phase_shift " ctf[cross_correlation]="cross_correlation " ctf[resolution]="resolution " ctf[resolution_performance]="resolution_performance;
  }
}'

}

motioncor_file()
{
  local input=$1
  local extension="${input##*.}"
  local basename=$(basename -- "$input")
  # sometimes it has the extention?!
  local datafile="${input}.log0-Patch-Full.log"
  if [[ $basename == FoilHole_* || $extension == 'mrc' ]]; then
    datafile="${input%.${extension}}.log0-Patch-Full.log"
  elif [[ "$GAINREF_FILE" != "" ]]; then
    datafile="${input%.${extension}}.log0-Patch-Full.log"
  fi
  echo $datafile
}

parse_motioncor()
{
  local input=$1
  local datafile=$(motioncor_file "$input") 
  if [ ! -e $datafile ]; then
    other_datafile="${datafile%.log0-Patch-Full.log}.mrc.log0-Patch-Full.log"
    if [ ! -e $other_datafile ]; then
      >&2 echo "motioncor2 data file $datafile does not exist"
      exit 4
    else
      datafile=$other_datafile
    fi
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
  f1 = sprintf("%.4f", drifts[1]);
  f3 = sprintf("%.5f", first3/3);
  f5 = sprintf("%.4f", first5/5);
  f8 = sprintf("%.4f", first8/8);
  a = sprintf("%.4f", all/length(drifts));
  print "declare -A align; align[first1]="f1 " align[first3]="f3 " align[first5]="f5 " align[first8]="f8 " align[all]="a " align[frames]="length(drifts)+1;
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


