#!/bin/bash

#trap "trap - SIGTERM && /bin/kill -- -$(ps -o pgid= $$ | grep -o '[0-9]*')" SIGINT SIGTERM EXIT

TOKEN=unset
FILEPATH=unset
CHANNEL_NAME=unset
FILENAME=unset
FILELENGTH=unset
#JQ_PATH=$(which jq)
#JQ_EXE=${JQ_PATH:-/usr/local/airflow/dags/jq}
JQ_EXE=/usr/local/airflow/dags/jq

get_channel_id() {
  CHANNEL_NAME=$1
  CHANNEL_LIST=()
  CHANNEL_ID=""
  NEXT_CURSOR=""
  while true; do
    RESPONSE=$(curl -s -H "Authorization: Bearer ${TOKEN}" "https://slack.com/api/conversations.list?limit=1000&types=private_channel&exclude_archived=true&cursor=${NEXT_CURSOR}")
    NUM_CHANNELS=$("${JQ_EXE}" '.channels | length' <(echo "${RESPONSE}"))
    if [ "${NUM_CHANNELS}" -gt 0 ]; then
      CHANNEL_ID=$("${JQ_EXE}" -r ".channels[] | select(.name | contains(\"${CHANNEL_NAME}\")) | .id" <(echo "${RESPONSE}"))
      if [ ! -z "${CHANNEL_ID}" ]; then
        break
      fi
    fi
  
    RESPONSE_METADATA=$(echo "${RESPONSE}" | "${JQ_EXE}" -r ". | select(.response_metadata != \"\" and .response_metadata != null) | .response_metadata")
    if [ "${RESPONSE_METADATA}" != "{}" ]; then
      if [ ! -z $(echo "${RESPONSE_METADATA}" | "${JQ_EXE}" -r ".next_cursor") ]; then
        NEXT_CURSOR=$(echo "${RESPONSE_METADATA}" | "${JQ_EXE}" -r ".next_cursor")
        continue
      else
        NEXT_CURSOR=""
        break
      fi 
    else
      NEXT_CURSOR=""
      break
    fi
  
    if [ -z "${NEXT_CURSOR}" ]; then
      break
    fi
    
  done
  
  if [ -z "${CHANNEL_ID}" ]; then
    echo "Error: channel ID for channel ${CHANNEL_NAME} not found."
    exit 1
  else
    echo "${CHANNEL_ID}"
  fi
}

usage() {
  echo "Usage: 
        slack_preview_upload.sh -t | --token TOKEN
                                -f | --file FILEPATH
                                -c | --channel CHANNEL_NAME
                                -h | --help"
  exit 1
}

main() {
  if [ "$#" -lt 3 ]; then
    usage
  fi
  
  PASSED_ARGUMENTS=$(getopt -o t:f:c:h --long token:,file:,channel:,help -- "$@")
  VALID_ARGUMENTS=$?
  if [ "${VALID_ARGUMENTS}" != "0" ]; then
    usage
  fi
  
  eval set -- "${PASSED_ARGUMENTS}"
  
  while true; do
    case "$1" in
      -t | --token) TOKEN="$2" ; shift 2 ;;
      -f | --file) FILEPATH="$2" ; shift 2 ;;
      -c | --channel) CHANNEL_NAME="$2"; shift 2 ;;
      -h | --help) usage; shift ; break ;;
      --) shift; break ;;
      *) shift; break ;;
    esac
  done
  
  echo "TOKEN=${TOKEN}"
  echo "FILE=${FILEPATH}"
  
  if [ -f "${FILEPATH}" ]; then
    FILENAME=$(basename ${FILEPATH})
    FILELENGTH=$(stat --printf="%s" ${FILEPATH})
  else
    echo "No file found at ${FILEPATH}, exiting."
    exit 1
  fi
  
  echo "FILENAME=${FILENAME}"
  echo "FILELENGTH=${FILELENGTH}"
  
  RESPONSE=$(curl -s -H "Authorization: Bearer ${TOKEN}" "https://slack.com/api/files.getUploadURLExternal?filename=${FILENAME}&length=${FILELENGTH}")
  ERROR=$(echo "${RESPONSE}" | "${JQ_EXE}" -r ". | select(.ok == false) | .error")    
  echo "files.getUploadURLExternal HTTP RESPONSE: ${RESPONSE}"
  if [ -z "${ERROR}" ]; then
    UPLOAD_URL=$(echo "${RESPONSE}" | "${JQ_EXE}" -r ". | select(.ok == true) | .upload_url")
    echo "UPLOAD_URL=${UPLOAD_URL}"
    FILE_ID=$(echo "${RESPONSE}" | "${JQ_EXE}" -r ". | select(.ok == true) | .file_id")
    echo "FILE_ID=${FILE_ID}"
    RESPONSE=$(curl -H "Authorization: Bearer ${TOKEN}" -F "filename=@${FILEPATH}" -X POST "${UPLOAD_URL}")
    echo "File upload HTTP RESPONSE: ${RESPONSE}"
    CHANNEL_ID=$(get_channel_id "${CHANNEL_NAME}")
    echo "CHANNEL_ID=${CHANNEL_ID}"
    JSON_DATA="{\"files\": [{\"id\": \"${FILE_ID}\", \"title\": \"${FILENAME}\"}], \"channel_id\": \"${CHANNEL_ID}\"}"
    echo "JSON_DATA=${JSON_DATA}"
    RESPONSE=$(curl -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" -X "POST" -d "${JSON_DATA}" "https://slack.com/api/files.completeUploadExternal")
    echo "files.completeUploadExternal HTTP RESPONSE: ${RESPONSE}"
    exit 0
  else 
    echo "Slack file upload error: ${ERROR}"
    exit 2
  fi
}

set -e
main "$@"
