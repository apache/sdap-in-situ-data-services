export IMAGE_PREFIX ?= waiphyojpl
export NAME ?= uds
export LAMBDA_NAME ?= parquet_lambda
export VERSION ?= latest


all: build_lambda upload_lambda update_lambda_function build_docker
local: build_lambda local_upload
local_lambda_docker: build_lambda_docker local_lambda_docker_upload


build_lambda_docker:
	docker build -t "$(IMAGE_PREFIX)/$(LAMBDA_NAME):$(VERSION)" -f docker/parquet.lambda.Dockerfile .

local_lambda_docker_upload:
	DOCKER_IMAGE="$(IMAGE_PREFIX)/$(LAMBDA_NAME)" DOCKER_TAG=${VERSION} /bin/bash ci.cd/lambda_docker_upload.sh

lambda_docker_push_docker_hub:
	docker push "$(IMAGE_PREFIX)/$(LAMBDA_NAME):$(VERSION)"

build_lambda:
	docker run --rm -v `PWD`:"/usr/src/app/cdms_lambda_functions":z -w "/usr/src/app/cdms_lambda_functions" cae-artifactory.jpl.nasa.gov:17001/python:3.7 ci.cd/create_s3_zip.sh

local_upload:
	/bin/bash ci.cd/local_upload.sh

build_lambda_public:
	docker run --rm -v `PWD`:"/usr/src/app/cdms_lambda_functions":z -w "/usr/src/app/cdms_lambda_functions" python:3.8 ci.cd/create_s3_zip.sh

upload_lambda:
	aws --profile saml-pub s3 cp cdms_lambda_functions__0.1.1.zip s3://cdms-dev-in-situ-parquet/cdms_lambda/

update_lambda_function:
	aws --profile saml-pub lambda update-function-code --zip-file fileb:///Users/wphyo/Projects/access/parquet_test_1/cdms_lambda_functions__0.1.1.zip --function-name arn:aws:lambda:us-west-2:848373852523:function:cdms-dev-in-situ-parquet-ingestion-v1 --publish
