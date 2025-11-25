#!/bin/bash
# deploy_lambda.sh - Construye la imagen Docker de la Lambda, la sube a ECR y crea/actualiza la función Lambda.

PROFILE="default"
BUCKET=""
REPO_NAME="meteogalicia-processor"
FUNCTION_NAME="MeteoGaliciaProcessor"
PREFIX="meteo_parquet"

wait_for_update() {
  local function_name="$1"
  local attempts=0
  local status=""
  while true; do
    status=$(aws lambda get-function-configuration --function-name "$function_name" --profile "$PROFILE" --query 'LastUpdateStatus' --output text 2>/dev/null)
    if [[ "$status" == "Successful" ]]; then
      return 0
    fi
    if [[ "$status" == "Failed" ]]; then
      echo "Error: Lambda update failed for $function_name (LastUpdateStatus=Failed)" >&2
      return 1
    fi
    attempts=$((attempts+1))
    if [[ $attempts -ge 30 ]]; then
      echo "Error: Timeout waiting for Lambda $function_name to finish updating (status=$status)" >&2
      return 1
    fi
    echo "Lambda $function_name update in progress (status=$status); waiting..."
    sleep 2
  done
}

NO_CACHE=false
while getopts ":p:b:r:f:x:k" opt; do
  case $opt in
    p) PROFILE=$OPTARG ;;
    b) BUCKET=$OPTARG ;;
    r) REPO_NAME=$OPTARG ;;
    f) FUNCTION_NAME=$OPTARG ;;
    x) PREFIX=$OPTARG ;;
    k) NO_CACHE=true ;;
    \?) echo "Uso: $0 [-p perfil] -b <bucket> [-r repo_ecr] [-f nombre_funcion] [-x prefijo_s3] [-k (no-cache build)]"; exit 1 ;;
  esac
done

if [[ -z "$BUCKET" ]]; then
  echo "Error: Debe especificar el bucket de S3 destino con -b"
  exit 1
fi

# Obtener Account ID y Region
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)
REGION=$(aws configure get region --profile "$PROFILE")
REPO_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME"

echo "Iniciando construcción de la imagen Docker para la función Lambda..."
# Construir la imagen Docker localmente (opcional --no-cache)
build_opts=""
if [[ "$NO_CACHE" == true ]]; then
  build_opts="--no-cache"
fi
docker build $build_opts -t "$REPO_NAME:latest" ./lambda
if [[ $? -ne 0 ]]; then
  echo "Error: Falló la construcción de la imagen Docker"
  exit 1
fi

# Login a ECR
echo "Autenticando Docker con ECR..."
aws ecr get-login-password --region "$REGION" --profile "$PROFILE" | \
  docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"
if [[ $? -ne 0 ]]; then
  echo "Error: Falló la autenticación con Amazon ECR"
  exit 1
fi

# Crear repositorio ECR si no existe
echo "Creando repositorio ECR $REPO_NAME si no existe..."
aws ecr describe-repositories --repository-names "$REPO_NAME" --profile "$PROFILE" --region "$REGION" > /dev/null 2>&1
if [[ $? -ne 0 ]]; then
  aws ecr create-repository --repository-name "$REPO_NAME" --region "$REGION" --profile "$PROFILE" \
    --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
  echo "Repositorio $REPO_NAME creado en ECR."
else
  echo "Repositorio $REPO_NAME ya existe en ECR."
fi

# Etiquetar la imagen local con el URI del repositorio ECR
docker tag "$REPO_NAME:latest" "$REPO_URI:latest"

# Push de la imagen a ECR
echo "Subiendo la imagen Docker a ECR..."
docker push "$REPO_URI:latest"
if [[ $? -ne 0 ]]; then
  echo "Error: Falló el push de la imagen a ECR"
  exit 1
fi
echo "Imagen subida a ECR: $REPO_URI:latest"

# Obtener ARN del rol de Lambda creado anteriormente
ROLE_ARN=$(aws iam get-role --role-name LambdaExecutionRole --profile "$PROFILE" --query Role.Arn --output text)
if [[ -z "$ROLE_ARN" ]]; then
  echo "Error: No se encontró el rol LambdaExecutionRole. Asegúrese de ejecutar create_roles.sh primero."
  exit 1
fi

# Determinar si la función Lambda ya existe
echo "Desplegando la función Lambda \"$FUNCTION_NAME\"..."
aws lambda get-function --function-name "$FUNCTION_NAME" --profile "$PROFILE" > /dev/null 2>&1
if [[ $? -ne 0 ]]; then
  # Crear función Lambda nueva
  aws lambda create-function --function-name "$FUNCTION_NAME" --package-type Image \
    --code ImageUri="$REPO_URI:latest" --role "$ROLE_ARN" --profile "$PROFILE" \
    --environment Variables="{DEST_BUCKET=$BUCKET,DEST_PREFIX=$PREFIX}" \
    --memory-size 1024 --timeout 900
  echo "Función Lambda creada: $FUNCTION_NAME"
else
  # Actualizar función Lambda existente (código e imagen)
  # Update function code
  aws lambda update-function-code --function-name "$FUNCTION_NAME" \
    --image-uri "$REPO_URI:latest" --publish --profile "$PROFILE"
  echo "Waiting for function code update to finish..."
  wait_for_update "$FUNCTION_NAME"
  # Retry updating function configuration (environment, memory, timeout) to avoid conflicts
  echo "Updating function configuration for environment variables, memory, and timeout..."
  attempts=0
  until aws lambda update-function-configuration \
      --function-name "$FUNCTION_NAME" \
      --environment Variables="{DEST_BUCKET=$BUCKET,DEST_PREFIX=$PREFIX}" \
      --memory-size 1024 --timeout 900 --profile "$PROFILE"; do
    attempts=$((attempts+1))
    if [[ $attempts -ge 5 ]]; then
      echo "Error: Failed to update function configuration after $attempts attempts."
      exit 1
    fi
    echo "Conflict detected, retrying update-function-configuration ($attempts)..."
    sleep 2
  done
  echo "Lambda function configuration updated: $FUNCTION_NAME"
fi
