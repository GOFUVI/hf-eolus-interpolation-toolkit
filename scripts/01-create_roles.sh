#!/bin/bash
# create_roles.sh - Crea roles IAM para Lambda y Step Functions.

PROFILE="default"
BUCKET=""
while getopts ":p:b:" opt; do
  case $opt in
    p) PROFILE=$OPTARG ;;
    b) BUCKET=$OPTARG ;;
    \?) echo "Uso: $0 [-p perfil] -b <bucket_S3_destino>"; exit 1 ;;
  esac
done

if [[ -z "$BUCKET" ]]; then
  echo "Error: Debe especificar el bucket de S3 destino con -b"
  exit 1
fi

# Obtener Account ID y Region para usar en ARNs
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)
REGION=$(aws configure get region --profile "$PROFILE")
if [[ -z "$ACCOUNT_ID" || -z "$REGION" ]]; then
  echo "Error obteniendo Account ID o Region del perfil $PROFILE"
  exit 1
fi

echo "Creando rol de Lambda..."
# Política de confianza para Lambda (permite a Lambda asumir el rol)
read -r -d '' TRUST_LAMBDA << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "lambda.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role --profile "$PROFILE" --role-name LambdaExecutionRole \
    --assume-role-policy-document "$TRUST_LAMBDA" \
    --description "Rol de ejecución de Lambda para procesamiento de MeteoGalicia"

# Adjuntar políticas administradas: logs de Lambda y XRay (para seguimiento) 
aws iam attach-role-policy --profile "$PROFILE" --role-name LambdaExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole 
aws iam attach-role-policy --profile "$PROFILE" --role-name LambdaExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess 

# Política inline para permitir acceso de la Lambda al bucket S3 (get/put objetos en el bucket especificado)
read -r -d '' LAMBDA_S3_POLICY << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::$BUCKET/*",
        "arn:aws:s3:::$BUCKET"
      ]
    }
  ]
}
EOF

aws iam put-role-policy --profile "$PROFILE" --role-name LambdaExecutionRole \
    --policy-name LambdaS3AccessPolicy --policy-document "$LAMBDA_S3_POLICY"

echo "Rol de ejecución de Lambda creado y políticas adjuntadas."

echo "Creando rol de Step Functions..."
# Política de confianza para Step Functions
read -r -d '' TRUST_STEPFUNCTIONS << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "states.$REGION.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role --profile "$PROFILE" --role-name StepFunctionsWorkflowRole \
    --assume-role-policy-document "$TRUST_STEPFUNCTIONS" \
    --description "Rol de Step Functions para orquestar el procesamiento MeteoGalicia"

# Política inline: permitir a Step Functions invocar cualquier función Lambda en esta cuenta (puede restringirse al ARN específico)
read -r -d '' SF_INVOKE_LAMBDA << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:$REGION:$ACCOUNT_ID:function:*"
    }
  ]
}
EOF

aws iam put-role-policy --profile "$PROFILE" --role-name StepFunctionsWorkflowRole \
    --policy-name SFInvokeLambdaPolicy --policy-document "$SF_INVOKE_LAMBDA"

echo "Rol de Step Functions creado y políticas adjuntadas."
echo "Todos los roles IAM necesarios se han creado correctamente."
