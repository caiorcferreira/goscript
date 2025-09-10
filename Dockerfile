ARG CI_REGISTRY
FROM $CI_REGISTRY/ifood/security/engineering/miscellaneous-docker-images/golang-builder:1.22.2 AS build-env

WORKDIR /opt
COPY . /opt

RUN task build

# ------------------------------- #
FROM $CI_REGISTRY/ifood/docker-images/golden/go:1-stable

WORKDIR /app/app

COPY --from=build-env /opt/app ./service

ENTRYPOINT ["/executor","/app/app/service"]

