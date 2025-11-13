#!/bin/bash

# 07-setup_aws_resources.sh
# Crea recursos AWS necesarios para ejecución en Batch (AWS CLI)

# Parámetros por defecto
PROFILE="default"
REGION="eu-west-3"
ECR_REPO="wind-interpolation"
QUEUE_NAME="wind-interp-queue"
COMPUTE_ENV_NAME="wind-interp-compute-env"
JOB_DEFINITION_NAME="wind-interp-job-def"

# Parseo de argumentos
while getopts "p:r:" opt; do
    case $opt in
        p) PROFILE="$OPTARG" ;;
        r) REGION="$OPTARG" ;;
        *) echo "Uso: $0 [-p perfil] [-r region]"; exit 1 ;;
    esac
done

## Do not exit immediately on errors so the build/push step always runs
echo "Usando perfil AWS: $PROFILE, región: $REGION"

# 1. Crear repositorio ECR
echo "Creando (si no existe) el repositorio ECR '$ECR_REPO'..."
aws ecr create-repository --repository-name $ECR_REPO --region $REGION --profile $PROFILE >/dev/null 2>&1 || echo "Repositorio ECR '$ECR_REPO' ya existe."

# 2. Crear roles IAM necesarios
echo "Creando rol service-linked AWS Batch (AWSServiceRoleForBatch)..."
aws iam create-service-linked-role --aws-service-name batch.amazonaws.com --profile $PROFILE 2>/dev/null || echo "El rol AWSServiceRoleForBatch ya existe."

echo "Creando rol ecsInstanceRole para instancias EC2..."
cat > ecs-instance-trust.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF
aws iam create-role --role-name ecsInstanceRole --assume-role-policy-document file://ecs-instance-trust.json --profile $PROFILE 2>/dev/null || echo "El rol ecsInstanceRole ya existe."
# Attach the EC2 Container Service role policy
aws iam attach-role-policy \
    --role-name ecsInstanceRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role \
    --profile "$PROFILE" 2>/dev/null || echo "Policy AmazonEC2ContainerServiceforEC2Role already attached to ecsInstanceRole."
# Ensure an instance profile exists
echo "Creating or confirming instance profile 'ecsInstanceRole'..."
aws iam create-instance-profile --instance-profile-name ecsInstanceRole --profile "$PROFILE" >/dev/null 2>&1 || echo "Instance profile ecsInstanceRole already exists."
# Attach the IAM role to the instance profile if not already present
ATTACH_COUNT=$(aws iam get-instance-profile \
    --instance-profile-name ecsInstanceRole --profile "$PROFILE" \
    --query 'length(InstanceProfile.Roles[?RoleName==`ecsInstanceRole`])' --output text)
if [ "$ATTACH_COUNT" -eq 0 ]; then
    echo "Associating IAM role 'ecsInstanceRole' with instance profile..."
    aws iam add-role-to-instance-profile \
        --instance-profile-name ecsInstanceRole \
        --role-name ecsInstanceRole \
        --profile "$PROFILE" || echo "Warning: could not attach ecsInstanceRole to instance profile; you may need to add it manually."
else
    echo "IAM role 'ecsInstanceRole' already associated with instance profile."
fi

echo "Creando rol ecsTaskExecutionRole..."
cat > ecs-task-trust.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF
aws iam create-role --role-name ecsTaskExecutionRole --assume-role-policy-document file://ecs-task-trust.json --profile $PROFILE 2>/dev/null || echo "El rol ecsTaskExecutionRole ya existe."
aws iam attach-role-policy --role-name ecsTaskExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy \
    --profile "$PROFILE" 2>/dev/null || echo "Policy AmazonECSTaskExecutionRolePolicy already attached to ecsTaskExecutionRole."
# Grant read-write S3 access so Batch jobs can upload results to S3
echo "Attaching AmazonS3FullAccess to ecsTaskExecutionRole for S3 write operations..."
aws iam attach-role-policy --role-name ecsTaskExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
    --profile "$PROFILE" || echo "Policy AmazonS3FullAccess already attached to ecsTaskExecutionRole."

# 3. Crear Compute Environment (Batch)
echo "Obteniendo subredes predeterminadas y security group..."
SUBNET_IDS=$(aws ec2 describe-subnets \
    --filters Name=defaultForAz,Values=true \
    --query 'join(`,`, Subnets[].SubnetId)' \
    --output text --profile "$PROFILE" --region "$REGION")
SEC_GRP=$(aws ec2 describe-security-groups --filters Name=group-name,Values=default --query "SecurityGroups[0].GroupId" --output text --profile $PROFILE --region $REGION)
echo "Subredes: $SUBNET_IDS"
echo "Security Group: $SEC_GRP"

echo "Creando entorno de cómputo '$COMPUTE_ENV_NAME'..."
SERVICE_ROLE_ARN=$(aws iam get-role --role-name AWSServiceRoleForBatch --query Role.Arn --output text --profile $PROFILE)
if aws batch create-compute-environment --compute-environment-name "$COMPUTE_ENV_NAME" \
    --type MANAGED --state ENABLED \
    --compute-resources type=EC2,minvCpus=0,maxvCpus=16,desiredvCpus=0,instanceTypes=[m5.large],subnets=[$SUBNET_IDS],securityGroupIds=[$SEC_GRP],instanceRole=ecsInstanceRole \
    --service-role "$SERVICE_ROLE_ARN" \
    --region "$REGION" --profile "$PROFILE"; then
    echo "Compute environment '$COMPUTE_ENV_NAME' created."
else
    echo "Compute environment '$COMPUTE_ENV_NAME' already exists; skipping update (immutable settings)."
fi

# Wait for the compute environment to become VALID before creating the job queue
echo "Waiting for compute environment '$COMPUTE_ENV_NAME' to become VALID..."
# Poll until the compute environment transitions to VALID or reports an error
echo "Esperando a que el entorno de cómputo '$COMPUTE_ENV_NAME' sea válido..."
for i in {1..60}; do
    read STATUS REASON <<<$(aws batch describe-compute-environments \
        --compute-environments "$COMPUTE_ENV_NAME" \
        --query 'computeEnvironments[0].[status, statusReason]' \
        --output text --region "$REGION" --profile "$PROFILE")
    if [[ "$STATUS" == "VALID" ]]; then
        echo "Entorno de cómputo '$COMPUTE_ENV_NAME' es ahora válido."
        break
    elif [[ "$STATUS" == "INVALID" ]]; then
        echo "Error: compute environment '$COMPUTE_ENV_NAME' is INVALID." >&2
        if [[ -n "$REASON" ]]; then
            echo "Reason: $REASON" >&2
        fi
        echo "Full describe output:" >&2
        aws batch describe-compute-environments \
            --compute-environments "$COMPUTE_ENV_NAME" \
            --region "$REGION" --profile "$PROFILE" --output json >&2
        exit 1
    else
        echo "Estado actual: $STATUS. Esperando 5 segundos..."
        sleep 5
    fi
done
if [[ "$STATUS" != "VALID" ]]; then
    echo "Error: compute environment '$COMPUTE_ENV_NAME' did not become VALID in time." >&2
    exit 1
fi

# 4. Crear Job Queue
echo "Creando cola de trabajos '$QUEUE_NAME'..."
if aws batch create-job-queue --job-queue-name "$QUEUE_NAME" --state ENABLED --priority 1 \
    --compute-environment-order order=1,computeEnvironment="$COMPUTE_ENV_NAME" \
    --profile "$PROFILE" --region "$REGION"; then
    echo "Cola de trabajos '$QUEUE_NAME' creada correctamente."
else
    echo "La cola de trabajos '$QUEUE_NAME' ya existe."
fi

# 5. Registrar Job Definition
echo "Registrando definición de trabajo '$JOB_DEFINITION_NAME'..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --profile $PROFILE --region $REGION)
ECR_URI=$(aws ecr describe-repositories --repository-names $ECR_REPO --query "repositories[0].repositoryUri" --output text --profile $PROFILE --region $REGION)
JOB_ROLE_ARN=$(aws iam get-role --role-name ecsTaskExecutionRole --query Role.Arn --output text --profile $PROFILE)
EXEC_ROLE_ARN=$JOB_ROLE_ARN

cat > jobdef.json <<EOF
{
    "jobDefinitionName": "$JOB_DEFINITION_NAME",
    "type": "container",
    "containerProperties": {
        "image": "$ECR_URI:latest",
        "vcpus": 2,
        "memory": 7000,
        "jobRoleArn": "$JOB_ROLE_ARN",
        "executionRoleArn": "$EXEC_ROLE_ARN"
    }
}
EOF
aws batch register-job-definition --cli-input-json file://jobdef.json --profile $PROFILE --region $REGION

# 6. Construir y subir imagen Docker a ECR
echo "Construyendo imagen Docker y subiendo a ECR..."
aws ecr get-login-password --region $REGION --profile $PROFILE | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com
docker build -t $ECR_REPO .
docker tag $ECR_REPO:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$ECR_REPO:latest
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$ECR_REPO:latest

echo "Configuración de AWS Batch completada."
