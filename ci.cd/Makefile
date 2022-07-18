export IMAGE_PREFIX ?= cae-artifactory.jpl.nasa.gov:16001/gov/nasa/jpl/unity
export NAME ?= uds
export VERSION ?= latest


all: build_lambda upload_lambda update_lambda_function build_docker

build_docker:
	docker build -t "$(IMAGE_PREFIX)/$(NAME):$(VERSION)" -f docker/Dockerfile .

zip_docker:
	docker save "$(IMAGE_PREFIX)/$(NAME):$(VERSION)" | gzip > "$(NAME)__$(VERSION).tar.gz"

build_lambda:
	docker run --rm -v `PWD`:"/usr/src/app/cdms_lambda_functions":z -w "/usr/src/app/cdms_lambda_functions" cae-artifactory.jpl.nasa.gov:17001/python:3.8 ci.cd/create_s3_zip.sh

build_lambda_public:
	docker run --rm -v `PWD`:"/usr/src/app/cdms_lambda_functions":z -w "/usr/src/app/cdms_lambda_functions" python:3.8 ci.cd/create_s3_zip.sh

upload_lambda:
	aws --profile saml-pub s3 cp cumulus_lambda_functions_deployment.zip s3://am-uds-dev-cumulus-tf-state/unity_cumulus_lambda/

update_lambda_function:
	aws --profile saml-pub lambda update-function-code --s3-key unity_cumulus_lambda/cumulus_lambda_functions_deployment.zip --s3-bucket am-uds-dev-cumulus-tf-state --function-name arn:aws:lambda:us-west-2:884500545225:function:Test1 --publish
