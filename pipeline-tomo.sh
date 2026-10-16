#!/bin/bash -e

#load modules 
IMOD_VERSION="5.1.11"
IMOD_LOAD="imod/${IMOD_VERSION}"
ARETOMO_VERSION="2.3.1"
ARETOMO_LOAD="aretomo3/${ARETOMO_VERSION}"
FFMPEG_VERSION="4.4.2"
FFMPEG_LOAD="ffmpeg/${FFMPEG_VERSION}"
PYLIBS_VERSION="1.1"
PYLIBS_LOAD="pylibs/${PYLIBS_VERSION}"
ISONET_VERSION="2.0.0"
ISONET_LOAD="isonet/${ISONET_VERSION}"

# GENERATE
# select mode/task and define terms
MODE=${MODE:-tomo} 
TASK=${TASK:-all} # task options: generate 3D reconstruction / generate preview
FORCE=${FORCE:-0}
NO_FORCE_GAINREF=${NO_FORCE_GAINREF:-0}
NO_PREAMBLE=${NO_PREAMBLE:-0}
MOVIE_FORMAT=${MOVIE_FORMAT:-tif} #default movie format is tif, can be changed by user input

# SCOPE PARAMS (set by the microscopes)
CS=${CS:-2.7}
KV=${KV:-300}
APIX=${APIX}
SUPERRES=${SUPERRES:-0}
PHASE_PLATE=${PHASE_PLATE:-0}
AMPLITUDE_CONTRAST=${AMPLITUDE_CONTRAST:-0.1}
MIN_TILT=${MIN_TILT:--60}
MAX_TILT=${MAX_TILT:-60}
LOWPASS=${LOWPASS:-40}

#ARETOMO PARAMETERS
CMD=${CMD:-0}
GPU=${GPU:-0 1 2 3} #ask s3df guys about this
MCPATCH=${MCPATCH:-4 4}
FMDOSE=${FMDOSE}
FMINT=${FMINT}
SPLITSUM=${SPLITSUM:-1}
DARKTOL=${DARKTOL:-0.75}
VOLZ=${VOLZ:--1}
ALIGNZ=${ALIGNZ:-0}
ATBIN=${ATBIN:-4}
FLIPGAIN=${FLIPGAIN:-1}
ATPATCH=${ATPATCH:-4 4}
WBP=${WBP:-1}
OUTIMOD=${OUTIMOD:-1}
MCBIN=${MCBIN:-1}
MCITER=${MCITER:-40}
MCTOL=${MCTOL:-0.001}
DARKTOL=${DARKTOL:-0.75}
TILT_KEEP_RATIO=${TILT_KEEP_RATIO:-0.9}
TILTCOR=${TILTCOR:-1}
CORR_CTF=${CORR_CTF:-1}
CTF_ARG=${CTF_ARG:-"-CorrCTF ${CORR_CTF} ${LOWPASS}"}
OUTXF=${OUTXF:-0}
FLIPVOL=${FLIPVOL}

# ISONET PARAMS
AMPLITUDE_CONTRAST=${AMPLITUDE_CONTRAST:-0.1}
ISO_GPU=${ISO_GPU:-0,1,2,3} #isonet format requires no spaces 

#help function: explains required and optional arguments 
usage() {
  cat <<__EOF__
Usage: $0 MDOC_FILE [MDOC_FILE ...]

Mandatory Arguments:
  [-a|--apix FLOAT]            use specified pixel size
  [-d|--fmdose FLOAT]          use specified fmdose in calculations
  [-i|--fmint FLOAT]          use specified fmint in calculations
  [-o|--movie-format STR]        user must input movie files format type
  [-v|--flipvol]                  flip the reconstructed volume along the z-axis (useful for some microscopes)

Optional Arguments:
  [-l|--lowpass FLOAT]           low-pass resolution cutoff in Angstroms (default: 40)
  [-g|--gainref GAINREF_FILE]  use specificed gain reference file
  [-e|--defect DEFECT_FILE]    use specificed defect reference file
  [-b|--basename STR]          output files names with specified STR as prefix
  [-k|--kev INT]               input micrograph was taken with INT keV microscope
  [-s|--superres]              input micrograph was taken in super-resolution mode (so we should half the number of pixels)
  [-p|--phase-plate]           input micrograph was taken using a phase plate (so we should calculate the phase)
  [-f|--force]                 reprocess all steps (ignore existing results).
  [-m|--mode [spa|tomo]]       pipeline to use: single particle analysis of tomography
  [-t|--task sum|align|reconstruct|preview|all] what to process; sum the stack, align the stack; just particle pick; reconstruct tomogram; generate preview of tomogram; or all

__EOF__
}

main() {

  #allows user to use short options instead of long options
  for arg in "$@"; do
    shift
    case "$arg" in
      "--help")    set -- "$@" "-h";;
      "--gainref") set -- "$@" "-g";;
      "--defect") set -- "$@" "-e";;
      "--basename") set -- "$@" "-b";;
      "--force")   set -- "$@" "-F";;
      "--apix")    set -- "$@" "-a";;
      "--fmdose")  set -- "$@" "-d";;
      "--fmint")   set -- "$@" "-i";;
      "--movie-format") set -- "$@" "-o";;
      "--lowpass") set -- "$@" "-l";;
      "--kev")     set -- "$@" "-k";;
      "--superres") set -- "$@" "-s";;
      "--phase-plate") set -- "$@" "-p";;
      "--mode")    set -- "$@" "-m";;
      "--task")    set -- "$@" "-t";;
      *)           set -- "$@" "$arg";;
   esac
  done

#assigning command line flags to variables
#Grace: changed defect file variable to e so that two variables weren't using d
  while getopts "Fhspm:t:l:g:b:a:d:k:e:c:i:o:v" opt; do
    case "$opt" in
    g) GAINREF_FILE="$OPTARG";;
    e) DEFECT_FILE="$OPTARG";;
    b) BASENAME="$OPTARG";;
    a) APIX="$OPTARG";;
    d) FMDOSE="$OPTARG";;
    k) KV="$OPTARG";;
    i) FMINT="$OPTARG";;
    o) MOVIE_FORMAT="$OPTARG";;
    l) LOWPASS="$OPTARG";;
    s) SUPERRES=1;;
    p) PHASE_PLATE=1;;
    F) FORCE=1;;
    m) MODE="$OPTARG";;
    t) TASK="$OPTARG";;
    h) usage; exit 0;;
    ?) usage; exit 1;;
    esac
  done

  GAINREF_FILE="${GAINREF_FILE:-$GAINREF}"
  OUTDIR="${OUTDIR:-aretomo3_output}" 
  RECONSTRUCTED_DIR="${OUTDIR}/reconstructed"
  PREVIEW_DIR="${OUTDIR}/preview"

  MDOCS=("${@:$OPTIND}")
  if [[ ${#MDOCS[@]} -eq 0 && -n "$INPUT" ]]; then
    mapfile -t MDOCS < <(compgen -G "$INPUT")
  fi
  if [ ${#MDOCS[@]} -lt 1 ]; then
    echo "Need input mdoc MDOC_FILE to continue..."
    usage
    exit 1
  fi
  
  # read from mdocs
  #if [[ "$MODE" == "tomo" && -z $MDOC ]]; then
  #  echo "Need mdoc filepath [-c|--mdoc] to continue..."
  #  usage
  #  exit 1
  #fi

#ensure all required variables are defined !! 
if [ -z "$APIX" ]; then # doesn't allow for apix to be undefined, exits if it is
    echo "Need pixel size [-a|--apix] to continue..."
    usage
    exit 1
  fi

if [ -z "$FMDOSE" ]; then # doesn't allow for fmdose to be undefined, exits if it is
    echo "Need fmdose [-d|--fmdose] to continue..."
    usage
    exit 1
  fi

if [ -z "$FMINT" ]; then
  echo "Need fmint [-i|--fmint FLOAT] to continue..."
  usage
  exit 1
fi

if [ -z "$MOVIE_FORMAT" ]; then # doesn't allow for movie_format to be undefined
  echo "Need movie format [-o|--movie-format] to continue..."
  usage
  exit 1
elif ! [[ "$MOVIE_FORMAT" =~ ^\.?(tiff|mrc|eer)$ ]]; then
  echo "Invalid movie format specified: $MOVIE_FORMAT. Valid options are: tif, .tif, mrc, .mrc, eer, .eer."
  exit 1
fi

if [ -z "$FLIPVOL" ]; then
  echo "Need flipvol [-v|--flipvol] to continue..."
  usage
  exit 1
fi


ensure_all_files() {
  local mdoc="$1"
  local mdoc_dir
  local path
  local name
  local name_no_ext
  local prefix
  local expected_files
  local counted_files
  local dir
  local movie_extension="${MOVIE_FORMAT#.}" #remove dot from extension if it exists

  mdoc_dir=$(dirname "$mdoc")
  path=$(awk '/SubFramePath = / { print $3; exit }' "$mdoc")
  if [[ -z "$path" ]]; then
    >&2 echo "Error: SubFramePath not found in $mdoc"
    exit 1
  fi

  if [[ "$path" == *\\* ]]; then
    name="${path##*\\}"
  else
    if [[ "$path" != /* ]]; then
      path="$mdoc_dir/$path"
    fi
    dir=$(dirname "$path")
    if [[ "$dir" != "$mdoc_dir" ]]; then
      >&2 echo "Error: SubFramePath is not in $mdoc"
      exit 1
    fi
    name=$(basename "$path")
  fi

  name_no_ext="${name%.*}"
  if [[ "$name_no_ext" =~ ^(.+)_([0-9]{3})(_.+)?$ ]]; then
    prefix="${BASH_REMATCH[1]}"
  else
    >&2 echo "Error: could not parse prefix and frame index from $name"
    exit 1
  fi

  # scan matching files to determine the highest three-digit frame index and count files
  local max_index=0
  counted_files=0
  while IFS= read -r -d '' file; do
    bn=$(basename "$file")
    remainder="${bn#"${prefix}"_}"
    num="${remainder%%_*}"
    if [[ "$num" =~ ^[0-9]{3}$ && ( "$remainder" == "$num" || "$remainder" == "$num"_* ) ]]; then #ensure num is in correct format 
      n=$((10#$num))
      counted_files=$((counted_files+1))
      if (( n > max_index )); then
        max_index=$n
      fi
    fi
  done < <(find "$mdoc_dir" -maxdepth 1 -type f -name "${prefix}_*.${movie_extension}" -print0)

  if (( counted_files == 0 )); then
    >&2 echo "Error: no matching ${movie_extension} movie files found for prefix ${prefix} in $mdoc_dir"
    exit 1
  fi

  expected_files=$((max_index)) #treats the highest fm num as the expected num of files

  # for debug 
  >&2 echo "ensure_all_files: mdoc_dir=$mdoc_dir prefix=$prefix expected_files=$expected_files counted_files=$counted_files"

  if [[ "$counted_files" -lt "$expected_files" ]]; then
    >&2 echo "Error: expected at least $expected_files files matching ${prefix}_[0-9][0-9][0-9]*.${movie_extension} in $mdoc_dir, found $counted_files"
    exit 1
  else 
    >&2 echo "All expected files are present in $mdoc_dir!"
  fi
}

#TO DO: add function to kick things off based on the mode
#and the task specified by the user, and call the appropriate functions 
#process each input mdoc file
#the function will ask if the mode is spa, then call do_spa function 
#if the mode is tomo, then call do_tomo function 
#else, print an error message and exit 
# - call do_tomo_reconstruct, do_tomo_denoise, or do_preview
for MDOC in "${MDOCS[@]}"; do

  # strip ./
  if [[ "$MDOC" = ./* ]]; then MDOC="${MDOC:2}"; fi
  if [[ "$MODE" == "tomo" ]]; then
      >&2 echo "MDOC: ${MDOC}"
      do_tomo_reconstruct 
  elif [[ "$MODE" == "spa" ]]; then
      do_spa
  else
    echo "Invalid mode specified: $MODE"
    usage
    exit 1
  fi
done

if [[ "$MODE" == "tomo" && ( "$TASK" == "denoise" || "$TASK" == "all" ) ]]; then
  do_tomo_denoise
fi
}


#alternative spa pipeline
do_spa()
{
  echo "single_particle_analysis: TO DO"
  #may end up just deleting all this because it is calling functions that are not defined in this script, and we don't need them for the tomo pipeline



  #if [ ${NO_PREAMBLE} -eq 0  ]; then
  #  do_prepipeline

  #  if [[ "$TASK" == "align" || "$TASK" == "sum" || "$TASK" == "all" ]]; then
  #    local force=${FORCE}
  #    if [ ${NO_FORCE_GAINREF} -eq 1 ]; then 
  #      FORCE=0
  #    fi
  #    do_gainref
  #    FORCE=$force
  #  fi
  #else
    # still need to determine correct gainref
  #  local force=${FORCE}
  #  FORCE=0
  #  GAINREF_FILE=$(process_gainref "$GAINREF_FILE") || exit $?
  #  FORCE=$force
  #fi

  #if [[ "$TASK" == "align" || "$TASK" == "all" ]]; then
  #  do_spa_align
  #fi
  #if [[ "$TASK" == "sum" || "$TASK" == "all" ]]; then
  #  do_spa_sum
  #fi
  #TO DO: Grace will be deprecating the picking task for spa pipeline
  #if [[ "$TASK" == "pick" || "$TASK" == "all" ]]; then
    # get the assumed pick file name
  #  if [ -z $ALIGNED_FILE ]; then
  #    ALIGNED_FILE=$(align_file ${MICROGRAPH})  || exit $?
  #  fi
  #  do_spa_pick
  #fi

  #if [[ "$TASK" == "preview" || "$TASK" == "all" ]]; then
  #  echo "  - task: preview"
  #  local start=$(date +%s.%N)
    # need to guess filenames
  #  if [ "$TASK" == "preview" ]; then
  #    ALIGNED_DW_FILE=$(align_dw_file ${MICROGRAPH}) || exit $?
      #echo "ALIGNED_DW_FILE: $ALIGNED_DW_FILE"
  #    ALIGNED_CTF_FILE=$(align_ctf_file "${MICROGRAPH}") || exit $?
      #echo "ALIGNED_CTF_FILE: $ALIGNED_CTF_FILE"
      #PARTICLE_FILE=$(particle_file ${ALIGNED_DW_FILE}) || exit $?
      #echo "PARTICLE_FILE: $PARTICLE_FILE"
  #    SUMMED_CTF_FILE=$(sum_ctf_file "${MICROGRAPH}") || exit $?
      # remove the _sum bit if SUMMED_FILE defined
  #    if [ ! -z $SUMMED_FILE ]; then
  #      SUMMED_CTF_FILE="${SUMMED_CTF_FILE%_sum_ctf.mrc}_ctf.mrc"
        #>&2 echo "SUMMED CTF: $SUMMED_CTF_FILE"
  #    fi
      #echo "SUMMED_CTF_FILE: $SUMMED_CTF_FILE"
  #  fi
  #  PREVIEW_FILE=$(generate_preview) || exit $?
  #  echo "    files:"
  #  dump_file_meta "${PREVIEW_FILE}" || exit $?
  #  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  #  echo "    duration: $duration"
  #  echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  #fi
}


#based on assigned tasks will direct code to necessary funcs
#- reconstruct - aretomo
#- generate preview - mrc2mp4
#- all
#create do_aretomo 
# print information about tilt series
# calls function inside of it, keep track of time
do_tomo_reconstruct() {
  if [ ${NO_PREAMBLE} -eq 0  ]; then
    do_prepipeline
    if [[ "$TASK" == "reconstruct" || "$TASK" == "denoise" || "$TASK" == "preview" || "$TASK" == "all" ]]; then
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

  echo "tomographic_analysis:"
  local reconstructed_dir="$RECONSTRUCTED_DIR"
  local preview_dir="$PREVIEW_DIR"
  local expected_tomogram="${reconstructed_dir}/$(basename "${MDOC%.*}")_Vol.mrc"

  if [[ "$TASK" != "reconstruct" && "$TASK" != "denoise" && "$TASK" != "preview" && "$TASK" != "all" ]]; then
    >&2 echo "Error: Invalid task specified: $TASK . Valid options are: reconstruct, denoise, preview, all."
    usage
    # here include rules for denoise/refine
    # should they be able to just denoise/refine no) need evn/odd yes) can use deconv if no evn/odd split (maybe?)
    exit 1
  fi

  if [[ "$TASK" == "reconstruct" || "$TASK" == "all" ]]; then
    ensure_all_files "$MDOC"
    echo "  - task: reconstruct"
    local start=$(date +%s.%N)
    if [[ -f "$expected_tomogram" ]]; then
      >&2 echo "Skipping Reconstruction...tomogram already exists: $expected_tomogram"
      TOMOGRAM="$expected_tomogram"
    else
      tomo_reconstruction "$MDOC" "$GAINREF_FILE" "$reconstructed_dir" || exit $?
      TOMOGRAM=$(tomogram "$MDOC" "$reconstructed_dir") || exit $?
    fi
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  fi

  if [[ "$TASK" == "preview" || "$TASK" == "all" ]]; then
    echo "  - task: preview" 
    local start=$(date +%s.%N)
    if [[ ! -f "$expected_tomogram" ]]; then
      >&2 echo "Error: tomogram file $expected_tomogram does not exist for preview generation."
      exit 1
    else
      TOMOGRAM="$expected_tomogram"
      local preview_path=$(generate_preview "$TOMOGRAM" "$preview_dir" "$LOWPASS") || exit $? 
      echo "    files generated in $preview_path:"
      # dump_file_meta "${preview_path}" || exit $? #is it ok to move this? it has weird outputs sometimes
    fi
    local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
    echo "    duration: $duration"
    echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
  fi
}

do_tomo_denoise() {
  local missing_tomograms=()
  local mdoc
  for mdoc in "${MDOCS[@]}"; do
    local mdoc_tomogram="${RECONSTRUCTED_DIR}/$(basename "${mdoc%.*}")_Vol.mrc"
    if [[ ! -f "$mdoc_tomogram" ]]; then
      missing_tomograms+=("$mdoc_tomogram")
    fi
  done

  if [[ ${#missing_tomograms[@]} -gt 0 ]]; then
    >&2 echo "Error: denoising requires reconstructed tomograms for all input mdocs. Missing:"
    >&2 echo "  ${missing_tomograms[@]}"
    exit 1
  fi

  echo "  - task: denoise"
  local start=$(date +%s.%N)
  local star_path=$(prepare_star "$RECONSTRUCTED_DIR") || exit $?
  local denoise_path=$(tomo_denoise "$star_path" "$RECONSTRUCTED_DIR/denoised") || exit $?
  echo "    files successfully generated in $denoise_path:"
  local duration=$( awk '{print $2-$1}' <<< "$start $(date +%s.%N)" )
  echo "    duration: $duration"
  >&2 echo "    executed_at: " $(date --utc +%FT%TZ -d @$start)
}

# do tomo_preview


# write out record of data
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

  # input mdoc instead of micrograph
  if [[ "$MODE" == 'tomo' ]] ; then
    echo "  - task: input_mdoc"
    echo "    files:"
    dump_file_meta "${MDOC}" || exit $?
  fi
}

function gen_template() {
  eval "echo \"$1\""
}

# calls the process gain ref func
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

#GRACE
process_gainref()
{
  # read in a file and spit out the appropriate gainref to actually use via echo as path
  local input=$1
  local outdir=${2:-.}
  if [[ "$input" == /* ]]; then outdir="$(dirname -- "$input")"; else mkdir -p "$outdir"; fi
  if [[ ${input:0:2} == './' ]]; then input=${input#./}; fi

  >&2 echo
  
  local filename=$(basename -- "$input")
  local extension="${filename##*.}"
  local output="$outdir/${filename}" 

  if [ ! -e "$input" ]; then
    >&2 echo "gainref file $input does not exist!"
    exit 4
  fi

  if [[ "$extension" == "dm4" ]]; then
  
    output="$outdir/${input%.$extension}.mrc"
    if [[ $FORCE == 1 || ! -e $output ]]; then
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
  
  #added in support for gainref files ending in .gain 
  elif [[ "$extension" == "gain" ]]; then

    output="$outdir/${input%.$extension}.mrc"
    if [[ "$output" = ././* ]]; then output="${output:4}"; fi
    if [[ "$output" = ./* ]]; then output="${output:2}"; fi
    if [[ $FORCE == 1 || ! -e $output ]]; then
      >&2 echo "converting gainref file $input to $output..."
      module load ${IMOD_LOAD} || exit $?
      tif2mrc "$input" "$output" 1>&2 || exit $?
    else
      >&2 echo "gainref file $output already exists"
    fi
  
  # TODO: this needs testing
  elif [[ "$extension" == 'mrc' && ! -e $output ]]; then
    
    >&2 echo "Error: output gainref file $output does not exist"
    
  fi
  
  echo $output
}

#original script contains a variety of functions called motioncor_file, align_file, etc.
#the purpose of these functions is to generate an expected output file name 
#so that the script has unified naming conventions for the output files, and can check if they exist or not
#potentially we will need to add one for aretomo3 outputs
#can add this in later if it is needed


# TYNIQUE
tomo_reconstruction() {
  #prev tomo_3D
  local input="${1:-$INPUT}"
  local gainref="${2:-$GAINREF}" 
  local outdir="${3:-OUTDIR}"
  local prefix="${input%.*}." # strip extension of mdoc

  # if [[ "$prefix" == "$filename" ]]; then
  #  prefix="$filename"
  # fi

  if [[ -z "$input" ]]; then
    >&2 echo "Error: no input mdoc provided for tomogram reconstruction."
    return 1
  fi

  if [[ ! -f "$input" ]]; then
    >&2 echo "Error: input mdoc $input does not exist."
    return 1
  fi

  mkdir -p $outdir

  >&2 echo "reconstructing tomogram from $input"
  
  local aretomo_cmd="
  AreTomo3 \
      -Cmd ${CMD} \
      -InPrefix ${prefix} \
      -InSuffix mdoc \
      -Gain ${gainref} \
      -OutDir ${outdir} \
      -Gpu ${GPU} \
      -PixSize $(echo $APIX | awk -v superres=$SUPERRES '{ if( superres=="1" ){ print $1/2 }else{ print $1 } }') \
      -TiltKeepRatio ${TILT_KEEP_RATIO} \
      -McBin ${MCBIN} \
      -McIter ${MCITER} \
      -McTol ${MCTOL} \
      -McPatch ${MCPATCH} \
      -FmDose ${FMDOSE} \
      -FmInt ${FMINT} \
      -SplitSum ${SPLITSUM} \
      -DarkTol ${DARKTOL} \
      -VolZ ${VOLZ} \
      -AlignZ ${ALIGNZ} \
      -AtBin ${ATBIN} \
      -FlipGain ${FLIPGAIN} \
      -AtPatch ${ATPATCH} \
      -Wbp ${WBP} \
      -OutXF ${OUTXF} \
      -TiltCor ${TILTCOR} \
      ${CTF_ARG} \
      -FlipVol ${FLIPVOL} \
      -OutImod ${OUTIMOD} \
      -kV ${KV} \
      -Cs ${CS}
  "

  reconstruct_command=$(gen_template "$aretomo_cmd") || exit $?
  >&2 echo "executing:" $reconstruct_command
  module load ${ARETOMO_LOAD} || exit $?
  >&2 eval "$reconstruct_command"
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    >&2 echo "Error: AreTomo3 exited with code $rc"
    return $rc
  fi

  local tomogram_mrc="$outdir/$(basename ${input%.*})_Vol.mrc" #mdoc basename becomes prefix for .mrc 

  if [[ ! -f "$tomogram_mrc" ]]; then
    >&2 echo "Warning: expected output $tomogram_mrc was not created."
    return 1
  fi
}

# find and return the reconstructed tomogram file
# - also output tomograms into a directory for star file generation (separate into EVN/ODD)
pre_ctf_tomogram() {
  local input=$1
  local outdir="${2:-OUTDIR}"
  local filename=$(basename -- "$input")
  local prefix="${filename%.*}" #remove extension
  local all_mrc=("$outdir"/*_Vol.mrc)
  if [[ "$prefix" == "$filename" ]]; then
    prefix="$filename"
  fi

  local tomogram_mrc="$outdir/$(basename ${input%.*})_Vol.mrc"
  if [[ -f "$tomogram_mrc" ]]; then
    echo "$tomogram_mrc"
    return 0
  elif [[ ${#all_mrc[@]} -gt 0 ]]; then # hard code error checker 
    >&2 echo "Warning: expected tomogram .mrc not found in $outdir, but found $all_mrc"
    return 0
  else
    >&2 echo "Error: no tomogram .mrc found in $outdir"
    return 1
  fi
}



# create function to prepare star file for tomo reconstruction
# - use the pre_ctf_tomogram outputs to generate star file for tomo reconstruction
# - create a directory for the tomograms needed for star files 
# - ensure this runs only once, when all tomo reconstruction is complete
prepare_star() {
    local input="${INPUT}"
    local prefix="${input%.*}." # strip extension of mdoc
    local star_name="${prefix}.star"
    local workdir="$PWD"
    local base="${1:-$RECONSTRUCTED_DIR}"
    local even="${2:-$base/EVN}"
    local odd="${3:-$base/ODD}"
    mkdir -p "$even" "$odd"

    # prepare star has no option to set output dir 
    local outdir="$workdir/output/reconstructed/denoised"
    mkdir -p "$outdir" && cd "$outdir" || exit 1


    #create error fall backs

    module load ${ISONET_LOAD} || exit $?

    #change star name to prefix.star 
    # - var APIX, CS, KV, AC (?), TILT_MIN, TILT_MAX 
    # - add rindefocus
    # - make func like org aretomo call
    isonet.py prepare_star \
        --even "$even" \
        --odd "$odd" \
        --full None \
        --star_name "$star_name" \
        --mask_folder None \
        --coordinate_folder None \
        --pixel_size {$APIX} \
        --cs {$CS} \
        --voltage {$KV} \
        --ac {$AMPLITUDE_CONTRAST} \
        --tilt_min -60 \
        --tilt_max 60 \
        --create_average False \
        --number_subtomos auto
  #create avg for true single tomo
}

# create function to denoise .star 
# - 
tomo_denoise() {
    local in
    local star="${1:-tomograms.star}"
    local outdir="${2:-$RECONSTRUCTED_DIR/denoised}"
    mkdir -p "$outdir"

    # create error fallbacks
    if [[ ! -f "$star" ]]; then 
      >&2 echo "Error: star file $star not found"
      return 1
    fi

    # for now, should use reconstruct task to use isonet (until pipeline can support single tomogram denoising)
    isonet.py denoise \
        --star_file "$star" \
        --gpuID "${ISO_GPU}" \
        --ncpus 16 \
        --arch unet-medium \
        --pretrained_model None \
        --cube_size 96 \
        --epochs 50 \
        --batch_size auto \
        --loss_func L2 \
        --save_interval 10 \
        --learning_rate 0.0003 \
        --learning_rate_min 0.0003 \
        --mixed_precision True \
        --CTF_mode network \
        --isCTFflipped False \
        --with_preview False \
        --prev_tomo_idx 1 \
        --output_dir "$outdir"
}





lowpass_filter() {
  local input="$1"
  local output="$2"
  local lowpass="${3:-40.0}"

  >&2 echo "executing: lowpass filtering" 1>&2
  module load ${PYLIBS_LOAD} || exit $?
  python3 - "$input" "$output" "$lowpass" <<'PY'

  import numpy as np
  from scipy.ndimage import gaussian_filter

  def lowpass_sigma_px(resolution_A, pixel_size_A):
      """Gaussian σ (pixels) that corresponds to a resolution cutoff in Å."""
      return resolution_A / (2 * np.pi * pixel_size_A)


  def lowpass_2d(img, resolution=40.0, pixel_size=6.8):
      """Gaussian low-pass of a 2D slice (returns float32).

      Args:
          img:        2D array (any dtype).
          resolution: cutoff resolution in Å — features smaller than this are
                      suppressed. Lower = less smoothing.
          pixel_size: Å per pixel of `img`.
      """
      f = img.astype(np.float32)
      # Gaussian low-pass in Fourier space equivalent:
      # σ_px = resolution / (2π * pixel_size)
      sigma_px = lowpass_sigma_px(resolution, pixel_size)
      return gaussian_filter(f, sigma_px).astype(np.float32)


  def lowpass_volume_2d(volume, resolution=40.0, pixel_size=6.8):
      """Apply lowpass_2d slice-by-slice over a (Z, Y, X) volume (float32)."""
      out = np.empty(volume.shape, dtype=np.float32)
      for z in range(volume.shape[0]):
          out[z] = lowpass_2d(volume[z], resolution, pixel_size)
      return out


  def lowpass_3d(vol, cutoff_fraction, pixel_size=1.0):
      """Apply a 3D low-pass filter.

      Parameters
      ----------
      vol : (N, N, N) array
      cutoff_fraction : float — cutoff as fraction of Nyquist (0-0.5)
      pixel_size : float — Å/pixel

      Returns
      -------
      filtered : (N, N, N) array
      """
      N = vol.shape[0]

      freqs = np.fft.fftfreq(N)
      fz, fy, fx = np.meshgrid(freqs, freqs, freqs, indexing='ij')
      freq_r = np.sqrt(fx**2 + fy**2 + fz**2)

      # Butterworth low-pass (order 4)
      order = 4
      lp = 1.0 / (1.0 + (freq_r / max(cutoff_fraction, 1e-6))**(2 * order))

      V = np.fft.fftn(vol)
      V *= lp
      return np.fft.ifftn(V).real.astype(np.float32)


  if __name__ == "__main__":
      # CLI: python lowpass_filter.py in.mrc out.mrc [resolution_A] [pixel_size_A]
      # Applies the 2D Gaussian low-pass to every slice. If pixel_size is omitted
      # it is read from the MRC header (falls back to 6.8 Å/px).
      import sys
      import mrcfile

      if len(sys.argv) < 3:
          print("Usage: python - in.mrc out.mrc [resolution_A] [pixel_size_A]", file=sys.stderr)
          sys.exit(1)

      in_path = sys.argv[1]
      out_path = sys.argv[2]
      res = float(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] not in ("", "null") else 40.0

      with mrcfile.open(in_path, permissive=True) as m:
          vol = np.asarray(m.data, dtype=np.float32)
          vs = m.voxel_size
          hdr_px = float(getattr(vs, 'x', 0) or 0)

      px = float(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] not in ("", "null") else (hdr_px if hdr_px > 1.01 else 6.8)
      if vol.ndim == 2:
          vol = vol[None]

      out = lowpass_volume_2d(vol, res, px)
      with mrcfile.new(out_path, overwrite=True) as mo:
          mo.set_data(out)
          mo.voxel_size = vs

      print(f"Wrote {out_path}  shape={out.shape}  cutoff={res} Å  px={px} Å/px  sigma={lowpass_sigma_px(res, px):.2f} px", file=sys.stderr)

PY
}

generate_preview() {
  local input="$1"
  local outdir="${2:-.}" # if no output dir is given, put in current dir
  local lowpass="${3:-40.0}"
  local frame_rate="${4:-4}" # default frame rate is 4 if not specified as arg
  local format="mp4" # default output format is mp4/only thing code supports

  # ensures mrc file is provided
  if [[ -z "$input" ]]; then
      >&2 echo "Error: No .mrc file provided as an argument."
      >&2 echo "Usage: $0 <mrc_file> [output_dir] [frame_rate]"
      exit 1
  fi

  # ensures the input file exists
  if [ ! -e "$input" ]; then
    >&2 echo "input file $input not found!"
    exit 1
  fi

  #create output directory
  local filename=$(basename -- "$input")
  local extension="${filename##*.}"
  local output="$outdir/${filename%.${extension}}.${format}" # output file name is same as input but with .mp4 extension
  mkdir -p "$outdir" #if new output direcory is initialized, create it

  # checks if output files already exist, if so, exits to avoid overwriting (necessary? in old code)
  if [ -e "$output" ]; then
    >&2 echo "preview file $output already exists!"
    exit 1
  else
    >&2 echo "generating preview of $input to $output..."
    module load ${IMOD_LOAD} || exit $?
    module load ${FFMPEG_LOAD} || exit $?

    # imod command to compute min/max densities
    alterheader -mmm "$input"

    # reads density info from the header and extracts min/max values
    local header_info=$(header "$input" 2>&1)
    local min_density=$(grep -i 'Minimum Density' <<< "$header_info" | awk -F'\.\.\.' '{print $NF}' | awk '{print $1}')
    local max_density=$(grep -i 'Maximum Density' <<< "$header_info" | awk -F'\.\.\.' '{print $NF}' | awk '{print $1}')

    if [[ $FORCE -eq 1 || ! -e $output ]]; then
      >&2 rm -f $output # just in case
      >&2 echo "generating preview of $input to $output..."

      tmpfile="$input"
      if [[ -n "$lowpass" ]]; then
        tmpfile="$outdir/${filename%.*}_lowpass.mrc"
        lowpass_filter "$input" "$tmpfile" "$lowpass" || exit $?
        if [[ ! -e "$tmpfile" ]]; then
          >&2 echo "could not create image $tmpfile... exiting..."
          exit 4
        fi
      fi

      echo "executing: .mrc to .tif conversion" 1>&2
      # imod command 
      mrc2tif -C "${min_density}","${max_density}" "$tmpfile" "$outdir/${filename}" 1>&2 || {
      rc=$?
      echo "mrc2tif exited with code $rc" >&2
      exit "$rc"
      }
      echo "executing: .tif to .mp4 conversion" 1>&2
      # ffmpeg module command
      ffmpeg -pattern_type glob -framerate "${frame_rate}" -i "$outdir/${filename}"'*.tif' -pix_fmt yuv420p "$output" 1>&2 || {
      rc=$?
      echo "ffmpeg exited with code $rc" >&2
      exit "$rc"
      }
      
      echo "cleaning up .tif files" 1>&2
      rm -f "$outdir/${filename}"*.tif

      if [ "$lowpass" != "" ]; then
        >&2 echo "rm -f $tmpfile"
        rm -f "$tmpfile" || exit $?
      fi
    else
      >&2 echo "preview file $output already exists!"
      exit 1
    fi
  fi
      
  # makes sure output is there
  if [ ! -e "$output" ]; then
    >&2 echo "could not generate .mp4 file $output!"
    exit 4
  fi

dump_file_meta "${output}" >&2 || exit $?
asecho "$output"
}

#generate/dump file meta (smth similar) *1370*
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

  local md5file="${1%.*}.md5"
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


set -e
main "$@"