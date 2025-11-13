#!/bin/bash
# create_state_machine.sh - Crea la Step Functions State Machine para el pipeline de MeteoGalicia.

PROFILE="default"
STATE_MACHINE_NAME="MeteoGaliciaStateMachine"
FUNCTION_NAME="MeteoGaliciaProcessor"

while getopts ":p:n:f:" opt; do
  case $opt in
    p) PROFILE=$OPTARG ;;
    n) STATE_MACHINE_NAME=$OPTARG ;;
    f) FUNCTION_NAME=$OPTARG ;;
    \?) echo "Uso: $0 [-p perfil] [-n nombre_state_machine] [-f nombre_funcion_lambda]"; exit 1 ;;
  esac
done

# Obtener ARNs necesarios
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)
REGION=$(aws configure get region --profile "$PROFILE")
LAMBDA_ARN="arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$FUNCTION_NAME"
# Obtain the Step Functions role ARN, try specified profile then default
ROLE_ARN=$(aws iam get-role --role-name StepFunctionsWorkflowRole --profile "$PROFILE" --query Role.Arn --output text 2>/dev/null || true)
if [[ -z "$ROLE_ARN" ]]; then
  echo "Role 'StepFunctionsWorkflowRole' not found under profile '$PROFILE'; trying default profile..."
  ROLE_ARN=$(aws iam get-role --role-name StepFunctionsWorkflowRole --query Role.Arn --output text 2>/dev/null || true)
  if [[ -z "$ROLE_ARN" ]]; then
    echo "Error: Role 'StepFunctionsWorkflowRole' not found."
    echo "Please run scripts/01-create_roles.sh first with -p <profile> -b <bucket>."
    exit 1
  fi
  echo "Using Role ARN from default profile: $ROLE_ARN"
fi

echo "Creando definición de la máquina de estados Step Functions..."

# Definición JSON de la máquina de estados (usando aqui-heredoc para insertar ARN de Lambda dinámicamente)
 read -r -d '' STATE_DEF << 'EOF'
{
  "Comment": "State machine para procesar archivos NetCDF de MeteoGalicia en rango de fechas",
  "StartAt": "ProcesarTodasLasFechas",
  "States": {
    "ProcesarTodasLasFechas": {
      "Type": "Map",
      "ResultPath": null,
      "OutputPath": null,
      "ItemsPath": "$.urlList",
      "Parameters": {
        "url.$":         "$$.Map.Item.Value",
        "regions.$":     "$.regions",
        "region_name.$": "$.regions[0].region_name",
        "polygon.$":     "$.regions[0].polygon",
        "test_points.$": "$.test_points",
        "source_model.$": "$$.Execution.Input.source_model"
      },
      "MaxConcurrency": 2,
      "Iterator": {
        "StartAt": "ProcesarUnaFecha",
        "States": {
          "ProcesarUnaFecha": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
              "FunctionName": "LAMBDA_ARN_PLACEHOLDER",
              "Payload.$":   "$"
            },
            "End": true
          }
        }
      },
      "End": true
    }
  }
}
EOF

## Substitute the real Lambda ARN into the state machine definition
STATE_DEF=${STATE_DEF//LAMBDA_ARN_PLACEHOLDER/$LAMBDA_ARN}
# Check if the state machine already exists and update if so
EXISTING_STM_ARN=$(aws stepfunctions list-state-machines --profile "$PROFILE" --query "stateMachines[?name=='$STATE_MACHINE_NAME'].stateMachineArn" --output text)
if [[ -n "$EXISTING_STM_ARN" ]]; then
  echo "State machine '$STATE_MACHINE_NAME' already exists (ARN: $EXISTING_STM_ARN), updating definition..."
  aws stepfunctions update-state-machine \
    --state-machine-arn "$EXISTING_STM_ARN" \
    --definition "$STATE_DEF" \
    --role-arn "$ROLE_ARN" \
    --profile "$PROFILE"
  echo "State machine '$STATE_MACHINE_NAME' updated successfully."
  exit 0
fi
# Crear la máquina de estado
aws stepfunctions create-state-machine --name "$STATE_MACHINE_NAME" --definition "$STATE_DEF" \
  --role-arn "$ROLE_ARN" --type STANDARD --profile "$PROFILE"

echo "Máquina de estados '$STATE_MACHINE_NAME' creada exitosamente."
