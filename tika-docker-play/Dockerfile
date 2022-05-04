FROM openjdk:8u332-jre-bullseye

RUN apt -q -y update && apt -q -y install tesseract-ocr

RUN mkdir /tika-bin && mkdir /tika-data
COPY tika-app-2.4.1-SNAPSHOT.jar /tika-bin
COPY testOCR.pdf /tika-data
RUN tesseract -v
CMD ["java", "-jar", "/tika-bin/tika-app-2.4.1-SNAPSHOT.jar", "/tika-data/testOCR.pdf"]