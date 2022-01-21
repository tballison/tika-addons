FROM amd64/openjdk:11.0.8-slim-buster as POPPLER_BUILDER

RUN mkdir /tika-bin
COPY target/tika-app-custom-docker-1.19-SNAPSHOT.jar /tika-bin/tika-app-custom-docker.jar

#find a more elegant way of grabbing this after we release it
#COPY tika-app-2.2.1.jar /tika-bin/tika-app.jar
COPY my-tika-config.xml /tika-bin/my-tika-config.xml
#ENTRYPOINT ["java","-cp","/tika-bin/*", "MyTikaCLI", "/tika-bin/my-tika-config.xml"]

#e.g.
#docker run -d -p 9998:9998