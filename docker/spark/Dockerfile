FROM ubuntu:latest

RUN echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
RUN echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
RUN apt-get update && apt-get install -y software-properties-common
RUN apt-add-repository ppa:webupd8team/java && apt-get update && apt-get install -y oracle-java7-installer libslf4j-java liblog4j1.2-java libprotobuf-java python

ADD spark-1.6.1-bin-hadoop2.6.tgz /etc
RUN ln -s /etc/spark-1.6.1-bin-hadoop2.6/ /etc/spark

ADD java/* /usr/share/java/

ADD spark.conf /etc/spark/conf/spark-defaults.conf
ENV SPARK_DIST_CLASSPATH=/usr/share/java/slf4j-nop-1.7.5.jar:/usr/share/java/slf4j-api-1.7.5.jar:/usr/share/java/jersey-core-1.9.1.jar:/usr/share/java/jersey-bundle-1.9.1.jar:/usr/share/java/log4j-1.2-1.2.17.jar:/usr/share/java/protobuf-2.5.0.jar

ADD start.sh /bin/start.sh

EXPOSE 7077

CMD /bin/start.sh
