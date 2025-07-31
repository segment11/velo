# docker build -t montplex/velo:1.0.0 .
# need `gradle jar` first
FROM docker.1ms.run/eclipse-temurin:21-jdk-jammy

WORKDIR /montplex
VOLUME /var/lib/velo

COPY build/libs/dyn /montplex/dyn
COPY build/libs/log4j2.xml /montplex/log4j2.xml
COPY build/libs/lib /montplex/lib
COPY build/libs/velo-1.0.0.jar /montplex/velo-1.0.0.jar
COPY build/libs/velo.properties /montplex/velo.properties

CMD java -Xmx1g -Xms1g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar