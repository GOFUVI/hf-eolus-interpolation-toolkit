# AWS Lambda base image for Python 3.9 (Amazon Linux 2)
FROM public.ecr.aws/lambda/python:3.9

# Install Python dependencies including GeoPandas and its dependencies via PyPI wheels
RUN python3.9 -m pip install --upgrade --no-cache-dir \
    numpy==1.26.4 \
    pandas==2.2.3 \
    pyarrow==14.0.2 \
    xarray==2024.7.0 \
    netCDF4==1.7.2 \
    boto3==1.40.60 \
    geopandas==1.0.1

# Copy the Lambda function source code to the application directory
COPY lambda_processor.py ${LAMBDA_TASK_ROOT}/

# Entry command (handler) - not required when using the AWS base image,
# can be defined in the Lambda configuration.
# For clarity, set the handler: Python file and handler function name
# Set the handler for the AWS Lambda Runtime Interface Client
ENV AWS_LAMBDA_FUNCTION_HANDLER=lambda_processor.lambda_handler
CMD ["lambda_processor.lambda_handler"]
