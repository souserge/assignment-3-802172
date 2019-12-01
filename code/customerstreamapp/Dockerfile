FROM maven:latest
LABEL Name=customersteamapp Version=0.0.1

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY pom.xml /usr/src/app
RUN mvn install

COPY src /usr/src/app/src
RUN mvn compile
RUN mvn package

CMD mvn exec:java \
    -Dexec.mainClass=mysimbdp.CustomerStreamPipeline
#     -Dexec.args="--output=./output/"