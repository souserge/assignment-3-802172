# This your assignment deployment report

it is a free form. you can add:

* how to deploy your system 


```
docker run -it --rm --name my-maven-project -v "$(pwd)":/usr/src/mymaven -w /usr/src/mymaven maven:3.3-jdk-8 \
mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-starter \
      -DarchetypeVersion=2.16.0 \
      -DgroupId=mysimbdp \
      -DartifactId=customerstreamapp \
      -Dversion="0.1" \
      -DinteractiveMode=false
```