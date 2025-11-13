#!/bin/bash
# run_pipeline.sh - Inicia la ejecución del pipeline Step Functions para un rango de fechas dado.

PROFILE="default"
STATE_MACHINE_NAME="MeteoGaliciaStateMachine"
START_DATE=""
END_DATE=""
MODEL_CHOICE="wrf4km"
# Support multiple region boundaries
GEOJSON_FILES=()
REGION_NAMES=()
# Support a single test points CSV file
TEST_POINTS_FILE=""

while getopts ":p:n:s:e:g:r:t:m:" opt; do
  case $opt in
    p) PROFILE=$OPTARG ;;
    n) STATE_MACHINE_NAME=$OPTARG ;;
    s) START_DATE=$OPTARG ;;
    e) END_DATE=$OPTARG ;;
    g) GEOJSON_FILES+=("$OPTARG") ;;
    r) REGION_NAMES+=("$OPTARG") ;;
    t) TEST_POINTS_FILE="$OPTARG" ;;
    m) MODEL_CHOICE=$OPTARG ;;
    \?) echo "Uso: $0 [-p perfil] [-n nombre_state_machine] -s YYYY-MM-DD -e YYYY-MM-DD [-g <area_boundary.geojson>] [-r <region_name>] [-t <test_points.csv>] [-m wrf4km|wrf1_3km|wrf1km]"; exit 1 ;;
  esac
done


	# Inform if a test points CSV was provided
	if [[ -n "$TEST_POINTS_FILE" ]]; then
	  echo "Loaded test points CSV: $TEST_POINTS_FILE"
	fi

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "Error: Debe especificar fecha de inicio (-s) y fin (-e) en formato YYYY-MM-DD"
  exit 1
fi

case "$MODEL_CHOICE" in
  wrf4km|wrf1_3km|wrf1km) ;;
  *)
    echo "Error: modelo inválido '$MODEL_CHOICE'. Use wrf4km, wrf1_3km o wrf1km."
    exit 1
    ;;
esac
echo "Usando punto de descarga MeteoGalicia: $MODEL_CHOICE"

# Obtener ARN de la state machine
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines --profile "$PROFILE" --query "stateMachines[?name=='$STATE_MACHINE_NAME'].stateMachineArn" --output text)
if [[ -z "$STATE_MACHINE_ARN" ]]; then
  echo "Error: Máquina de estados '$STATE_MACHINE_NAME' no encontrada. Asegúrese de haberla creado con create_state_machine.sh"
  exit 1
fi

# Generar el JSON de input con lista de URLs usando el script Python 3
echo "Generando lista de URLs de NetCDF desde $START_DATE hasta $END_DATE..."
if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 is required but not found in PATH" >&2
  exit 1
fi
if [[ ${#GEOJSON_FILES[@]} -gt 0 || -n "$TEST_POINTS_FILE" ]]; then
  # If names provided, ensure they match boundaries count
  if [[ ${#REGION_NAMES[@]} -gt 0 && ${#REGION_NAMES[@]} -ne ${#GEOJSON_FILES[@]} ]]; then
    echo "Error: number of region names (-r) must match number of GeoJSON boundaries (-g)." >&2
    exit 1
  fi
  # Build arguments: dates, then each boundary with optional name
  ARGS=("-m" "$MODEL_CHOICE" "$START_DATE" "$END_DATE")
	  for idx in "${!GEOJSON_FILES[@]}"; do
	    ARGS+=("${GEOJSON_FILES[$idx]}")
	    if [[ $idx -lt ${#REGION_NAMES[@]} ]]; then
	      ARGS+=("${REGION_NAMES[$idx]}")
	    fi
	  done
	# Include test points CSV path if provided
	if [[ -n "$TEST_POINTS_FILE" ]]; then
	  ARGS+=("$TEST_POINTS_FILE")
	fi
	INPUT_JSON=$(python3 utils/generate_urls.py "${ARGS[@]}")
else
  INPUT_JSON=$(python3 utils/generate_urls.py -m "$MODEL_CHOICE" "$START_DATE" "$END_DATE")
fi
if [[ $? -ne 0 || -z "$INPUT_JSON" ]]; then
  echo "Error generando la lista de URLs o leyendo GeoJSON. Verifique los parámetros."
  exit 1
fi

# Iniciar la ejecución del Step Function con el JSON generado
echo "Iniciando ejecución de Step Functions..."
EXECUTION_ARN=$(aws stepfunctions start-execution --state-machine-arn "$STATE_MACHINE_ARN" \
               --input "$INPUT_JSON" --profile "$PROFILE" --query executionArn --output text)

if [[ -z "$EXECUTION_ARN" ]]; then
  echo "Error: No se pudo iniciar la ejecución de la máquina de estados."
  exit 1
fi

echo "Ejecución iniciada exitosamente. ARN de la ejecución: $EXECUTION_ARN"

echo "Puede monitorear el estado de la ejecución en la consola de Step Functions o usando AWS CLI."
  # Monitoreo de la ejecución hasta su finalización
  echo "Monitoreando ejecución de Step Functions..."
  while true; do
    # Obtener estado de la ejecución
    STATUS=$(aws stepfunctions describe-execution --execution-arn "$EXECUTION_ARN" --profile "$PROFILE" --query status --output text)
    echo "Estado actual: $STATUS"
    if [[ "$STATUS" == "RUNNING" ]]; then
      sleep 10;
      continue;
    fi;
    break;
  done;
  if [[ "$STATUS" == "SUCCEEDED" ]]; then
    echo "Ejecución completada con éxito.";
    exit 0;
  else
    echo "Ejecución finalizada con estado: $STATUS"
    echo "Obteniendo detalles de la ejecución para log..."
    # Crear directorio de logs si no existe
    LOG_DIR="logs"
    mkdir -p "$LOG_DIR"
    # Archivo de log con detalles de la ejecución (se sobrescribe)
    LOG_FILE="$LOG_DIR/execution_details.json"
    # Guardar detalles importantes: estado y output (incluye causa de error)
    aws stepfunctions describe-execution \
      --execution-arn "$EXECUTION_ARN" \
      --profile "$PROFILE" \
      --query '{status:status, startDate:startDate, stopDate:stopDate, output:output}' \
      --output json > "$LOG_FILE"
    echo "Detalles de la ejecución guardados en $LOG_FILE"
    # Archivo de historial completo de la ejecución (se sobrescribe)
    HISTORY_FILE="$LOG_DIR/execution_history.json"
    aws stepfunctions get-execution-history --execution-arn "$EXECUTION_ARN" --profile "$PROFILE" --output json > "$HISTORY_FILE"
    echo "Historial completo de la ejecución guardado en $HISTORY_FILE"
    # Obtener la causa del fallo del último ExecutionFailed event
    CAUSE=$(aws stepfunctions get-execution-history \
      --execution-arn "$EXECUTION_ARN" \
      --profile "$PROFILE" \
      --query "events[-1].executionFailedEventDetails.cause" \
      --output text)
    echo "$CAUSE" > "$LOG_DIR/failure_cause.txt"
    echo "Causa del fallo guardada en $LOG_DIR/failure_cause.txt"
    # Retrieve last 20 CloudWatch log lines for the Lambda function
    LAMBDA_LOG_GROUP="/aws/lambda/MeteoGaliciaProcessor"
    LAMBDA_LOG_FILE="$LOG_DIR/lambda_last_logs.txt"
    echo "Retrieving last 20 CloudWatch log lines for $LAMBDA_LOG_GROUP"
    aws logs filter-log-events \
      --region eu-west-3 \
      --log-group-name "$LAMBDA_LOG_GROUP" \
      --limit 20 \
      --query 'events[*].message' \
      --profile "$PROFILE" \
      --output text > "$LAMBDA_LOG_FILE" 2>&1
    echo "CloudWatch logs saved to $LAMBDA_LOG_FILE"
    exit 1
  fi
