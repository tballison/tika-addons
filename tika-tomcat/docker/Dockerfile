#FROM tomcat:8-jdk11-openjdk-slim
FROM tomcat:9.0.56-jdk11-openjdk
COPY SampleServlet.war /usr/local/tomcat/webapps
CMD ["catalina.sh", "run"]