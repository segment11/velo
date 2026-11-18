# syntax=docker/dockerfile:1
#
# Multi-arch image (linux/amd64 + linux/arm64), the image content is arch
# independent: the jar and all runtime deps are pure java, and the native libs
# (netty epoll, zstd-jni, rocksdbjni, jffi) ship both the linux-x86_64 and the
# linux-aarch64 variants inside the very same jars.
#
# Build it with buildx, need `./gradlew jar` first:
#   ./scripts/docker/build_multiarch.sh --push
#
# Or by hand:
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     --build-arg VELO_VERSION=1.2.0 \
#     -t registry.cn-guangzhou.aliyuncs.com/montplex/velo:1.2.0 --push .
#
ARG BASE_IMAGE=docker.1ms.run/eclipse-temurin:21-jdk-jammy
FROM ${BASE_IMAGE}

ARG VELO_VERSION
ENV VELO_VERSION=${VELO_VERSION}

WORKDIR /opt
VOLUME /var/lib/velo

# The jar manifest class-path is relative ("." and "lib/..."), so the jar and the
# lib dir must stay siblings and the server must run with /opt as the work dir,
# the dyn scripts are loaded from <work dir>/dyn/src and the configs from
# <work dir> (velo.properties, log4j2.xml, conf.properties).
COPY build/libs/dyn /opt/dyn
COPY build/libs/lib /opt/lib
COPY build/libs/log4j2.xml /opt/log4j2.xml
COPY build/libs/conf.properties /opt/conf.properties
COPY build/libs/velo.properties /opt/velo.properties
COPY build/libs/velo-${VELO_VERSION}.jar /opt/velo.jar

EXPOSE 7379

CMD java -Xmx1g -Xms1g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar /opt/velo.jar
