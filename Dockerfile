# docker build -t montplex/velo:1.0.0 .
# need `gradle jar` first
FROM docker.1ms.run/eclipse-temurin:21-jdk-jammy

WORKDIR /opt
VOLUME /var/lib/velo

COPY build/libs/dyn /opt/dyn
COPY build/libs/log4j2.xml /opt/log4j2.xml
COPY build/libs/lib /opt/lib
COPY build/libs/velo-1.0.0.jar /opt/velo-1.0.0.jar
COPY build/libs/velo.properties /opt/velo.properties

CMD java -Xmx1g -Xms1g -XX:+UseZGC -XX:+ZGenerational -jar velo-1.0.0.jar