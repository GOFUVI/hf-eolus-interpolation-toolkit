#!/bin/bash

# 08-run_interpolation.sh
# Envia trabajos a AWS Batch para interpolación de viento

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default values
PROFILE="default"
AWS_REGION=""
RES_FACTOR="1"
CUTOFF="5"
WIDTH="0.5"
SUBSAMPLE_PCT="10"
NFOLD="5"
NMAX_MODEL="16"
REGION_NAME=""
PLOTS_ROOT=""
VERBOSE=false
NO_PLOTS=false
DATE_LIST=()
HOUR_LIST=()
INPUT_PATH=""
OUTPUT_PATH=""
RUN_TESTS=true

# Función para imprimir uso
usage() {
    echo "Usage: $0 [-p profile] [-R aws_region] [-r res_factor] [-c cutoff_km] [-w width_km] [-n subsample_pct] [-F n_fold] [-m nmax_model] [-H hours] [--region-name name] [--plots-root plots_s3_path] [-v|--verbose] [--no-plots] [--skip-tests] --input-path S3_INPUT --output-path S3_OUTPUT [--start YYYY-MM-DD --end YYYY-MM-DD | --dates YYYY-MM-DD,YYYY-MM-DD,...]"
    exit 1
}


# Parseo de argumentos
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -v|--verbose) VERBOSE=true; shift ;;
        --no-plots) NO_PLOTS=true; shift ;;
        -p) PROFILE="$2"; shift 2 ;;
        -r) RES_FACTOR="$2"; shift 2 ;;
        -c|--cutoff) CUTOFF="$2"; shift 2 ;;
        -w|--width) WIDTH="$2"; shift 2 ;;
        -n|--subsample) SUBSAMPLE_PCT="$2"; shift 2 ;;
        --start) START_DATE="$2"; shift 2 ;;
        --end) END_DATE="$2"; shift 2 ;;
        --dates) IFS=',' read -r -a DATE_LIST <<< "$2"; shift 2 ;;
        -H|--hour) IFS=',' read -r -a HOUR_LIST <<< "$2"; shift 2 ;;
        --input-path) INPUT_PATH="$2"; shift 2 ;;
        --output-path) OUTPUT_PATH="$2"; shift 2 ;;
        -R|--aws-region) AWS_REGION="$2"; shift 2 ;;
        -F|--n-fold) NFOLD="$2"; shift 2 ;;
        -m|--nmax-model) NMAX_MODEL="$2"; shift 2 ;;
        --region-name) REGION_NAME="$2"; shift 2 ;;
        --plots-root) PLOTS_ROOT="$2"; shift 2 ;;
        --skip-tests) RUN_TESTS=false; shift ;;
        *) echo "Opción desconocida: $1"; usage ;;
    esac
done

# Validar argumentos y generar lista de fechas

# Determine appropriate date command for incrementing dates (GNU date or BSD date)
if command -v gdate >/dev/null 2>&1; then
    DATE_PROG=gdate
else
    DATE_PROG=date
fi

date_next() {
    if $DATE_PROG --version >/dev/null 2>&1; then
        # GNU date
        $DATE_PROG -I -d "$1 + 1 day"
    else
        # BSD date fallback
        date -j -f "%Y-%m-%d" -v+1d "$1" "+%Y-%m-%d"
    fi
}
if [[ -n "$START_DATE" && -n "$END_DATE" ]]; then
    current="$START_DATE"
    while [[ "$current" < "$END_DATE" ]] || [[ "$current" == "$END_DATE" ]]; do
        DATE_LIST+=("$current")
        current=$(date_next "$current")
    done
elif [[ ${#DATE_LIST[@]} -eq 0 ]]; then
    echo "Debe proporcionar --start y --end, o --dates."
    usage
fi

# Validate input/output paths
if [[ -z "$INPUT_PATH" || -z "$OUTPUT_PATH" ]]; then
    echo "Debe proporcionar --input-path y --output-path."
    usage
fi

# If no hours specified, default to all hours (00-23)
if [[ ${#HOUR_LIST[@]} -eq 0 ]]; then
    for h in {0..23}; do
        HOUR_LIST+=( "$(printf '%02d' "$h")" )
    done
fi

# Determine AWS region from argument or AWS configuration/env
if [[ -z "$AWS_REGION" ]]; then
    AWS_REGION=$(aws configure get region --profile "$PROFILE")
    AWS_REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-}}
fi

if [[ "$RUN_TESTS" = true ]]; then
    echo "Ejecutando tests antes de lanzar AWS Batch..."
    "${SCRIPT_DIR}/00-verify_tests.sh"
else
    echo "Los tests previos se omiten por petición del usuario (--skip-tests)."
fi

echo "Enviando trabajos AWS Batch con perfil '$PROFILE', región '$AWS_REGION', factor de resolución $RES_FACTOR, cutoff $CUTOFF km, width $WIDTH km, subsample ${SUBSAMPLE_PCT}%, entrada '$INPUT_PATH', salida '$OUTPUT_PATH' para fechas: ${DATE_LIST[@]} y horas: ${HOUR_LIST[@]}"
echo "  nfold: $NFOLD, nmax_model: $NMAX_MODEL, region-name: $REGION_NAME"

# Variables fijas (ajustar si cambian)
QUEUE_NAME="wind-interp-queue"
JOB_DEF_NAME="wind-interp-job-def"

# Enviar trabajos Batch y capturar IDs
JOB_IDS=()
for FECHA in "${DATE_LIST[@]}"; do
    for HOUR in "${HOUR_LIST[@]}"; do
        JOB_NAME="interp-${FECHA}-${HOUR}"
        echo "Enviando job $JOB_NAME..."

        # Build command override with optional flags
        cmd=( "Rscript" "wind_interpolation.R" )
        if [ "$VERBOSE" = true ]; then
            cmd+=( "--verbose" )
        fi
        if [ "$NO_PLOTS" = true ]; then
            cmd+=( "--no-plots" )
        fi
        if [ -n "$REGION_NAME" ]; then
            cmd+=( "--region-name" "$REGION_NAME" )
            if [ -n "$AWS_REGION" ]; then
                cmd+=( "--aws-region" "$AWS_REGION" )
            fi
        fi
        if [ -n "$PLOTS_ROOT" ]; then
            cmd+=( "--plots-root" "$PLOTS_ROOT" )
        fi
        cmd+=( "$FECHA" "$HOUR" "$RES_FACTOR" "$INPUT_PATH" \
               "$OUTPUT_PATH" "$CUTOFF" "$WIDTH" "$SUBSAMPLE_PCT" "$NFOLD" \
               "$NMAX_MODEL" )
        cmd_json=$(printf '%s\n' "${cmd[@]}" | jq -R . | jq -cs .)

        overrides="{\"command\":${cmd_json},\"environment\":[\
            {\"name\":\"FECHA\",\"value\":\"$FECHA\"},\
            {\"name\":\"HOUR\",\"value\":\"$HOUR\"},\
            {\"name\":\"RES_FACTOR\",\"value\":\"$RES_FACTOR\"},\
            {\"name\":\"INPUT_PATH\",\"value\":\"$INPUT_PATH\"},\
            {\"name\":\"OUTPUT_PATH\",\"value\":\"$OUTPUT_PATH\"},\
            {\"name\":\"CUTOFF_KM\",\"value\":\"$CUTOFF\"},\
            {\"name\":\"WIDTH_KM\",\"value\":\"$WIDTH\"},\
            {\"name\":\"SUBSAMPLE_PCT\",\"value\":\"$SUBSAMPLE_PCT\"},\
            {\"name\":\"NFOLD\",\"value\":\"$NFOLD\"},\
            {\"name\":\"NMAX_MODEL\",\"value\":\"$NMAX_MODEL\"}\
        ]}"
        JOB_ID=$(aws batch submit-job \
            --job-name "$JOB_NAME" \
            --job-queue "$QUEUE_NAME" \
            --job-definition "$JOB_DEF_NAME" \
            --container-overrides "$overrides" \
            --profile "$PROFILE" \
            --region "$AWS_REGION" \
            --query jobId \
            --output text)
        echo "Job enviado con ID $JOB_ID"
        JOB_IDS+=("$JOB_ID")
    done
done

# Fin envío de jobs

echo "Todos los trabajos enviados: ${JOB_IDS[*]}"

# Monitorear jobs
describe_jobs_in_chunks() {
    local query="$1"
    shift
    local -n _ids_ref=$1
    local results=()
    local chunk_size=100
    local total=${#_ids_ref[@]}
    local i=0
    while [[ $i -lt $total ]]; do
        local chunk=("${_ids_ref[@]:i:chunk_size}")
        local response
        response=$(aws batch describe-jobs --jobs "${chunk[@]}" --profile "$PROFILE" \
            --region "$AWS_REGION" --query "$query" --output text)
        if [[ -n "$response" ]]; then
            read -r -a parsed <<<"$response"
            results+=("${parsed[@]}")
        fi
        i=$((i + chunk_size))
    done
    echo "${results[@]}"
}

echo "Monitoreando jobs..."
# Esperar a que todos los jobs alcancen estado terminal
while true; do
    read -r -a STATUSES <<<"$(describe_jobs_in_chunks "jobs[*].status" JOB_IDS)"
    total=${#STATUSES[@]}
    terminal=0
    for st in "${STATUSES[@]}"; do
        if [[ "$st" == "SUCCEEDED" || "$st" == "FAILED" ]]; then
            ((terminal++))
        fi
    done
    if [[ "$terminal" -eq "$total" ]]; then
        break
    fi
    echo "Current job statuses: ${STATUSES[*]}. Waiting..."
    sleep 10
done
# Verificar jobs fallidos
read -r -a FAILED_IDS_ARR <<<"$(describe_jobs_in_chunks "jobs[?status=='FAILED'].jobId" JOB_IDS)"
if [[ ${#FAILED_IDS_ARR[@]} -gt 0 ]]; then
    FAILED_IDS="${FAILED_IDS_ARR[*]}"
    echo "Algunos jobs fallaron: $FAILED_IDS" >&2
    # Mostrar motivo de fallo
    aws batch describe-jobs --jobs $FAILED_IDS --profile "$PROFILE" \
        --region "$AWS_REGION" \
        --query "jobs[*].[jobId,statusReason]" --output text >&2
    # Iterate over each failed job and display its CloudWatch logs
    for JOB in $FAILED_IDS; do
        # Describe the failed job
        JOB_DESC=$(aws batch describe-jobs --jobs "$JOB" --profile "$PROFILE" --region "$AWS_REGION" --output json)
        # Extract log group and log stream from the job description
        # Extract the CloudWatch log group, defaulting if absent
        LOG_GROUP=$(echo "$JOB_DESC" | python3 -c 'import json,sys; d=json.load(sys.stdin); c=d["jobs"][0]["container"]; print(c.get("logConfiguration",{}).get("options",{}).get("awslogs-group",""))')
        if [[ -z "$LOG_GROUP" ]]; then
            echo "Log group not specified in job description; defaulting to /aws/batch/job" >&2
            LOG_GROUP="/aws/batch/job"
        fi
        LOG_STREAM=$(echo "$JOB_DESC" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["jobs"][0]["container"].get("logStreamName",""))')
        echo "=== Logs for job $JOB (group: $LOG_GROUP, stream: $LOG_STREAM) ===" >&2
        if [[ -n "$LOG_GROUP" && -n "$LOG_STREAM" ]]; then
            aws logs get-log-events --log-group-name "$LOG_GROUP" \
                --log-stream-name "$LOG_STREAM" --profile "$PROFILE" --region "$AWS_REGION" \
                --start-from-head --output text >&2
        else
            echo "Unable to retrieve CloudWatch logs for job $JOB: missing logGroup or logStream" >&2
        fi
    done
    exit 1
else
    echo "Todos los jobs finalizaron correctamente."
    exit 0
fi
