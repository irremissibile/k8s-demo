FROM openjdk:8-jdk

LABEL maintainer="ihnatov.d@gmail.com"

RUN apt-get update

WORKDIR /opt/server

COPY /target/kafka-twitter-producer-1.0-SNAPSHOT.jar twitter-producer.jar

ENTRYPOINT ["sh", "-c"]
CMD ["exec java -jar twitter-producer.jar"]
