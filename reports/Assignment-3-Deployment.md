# This your assignment deployment report

it is a free form. you can add:

* how to deploy your system 


```
mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-starter \
      -DarchetypeVersion=2.16.0 \
      -DgroupId=mysimbdp \
      -DartifactId=customerstreamapp \
      -Dversion="0.1" \
      -DinteractiveMode=false
```