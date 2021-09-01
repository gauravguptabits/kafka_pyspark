# kafka_pyspark

## The system user can publish any kind of text message into the system, but management want to have a Realtime view of top-5 users who are publishing obscene text messages.

## Commands

-    Run Kafka and zookeeper
`docker-compose -f docker-compose-expose.yml up`

-    Running spark app
`sudo chmod -R 777 ~/projects/bits_project/`

-    Run pyspark docker image [(8888, Jupyter), (8088, job tracker), (4040, spark app UI)]
`docker run -it --rm -p 8888:8888 -p 8088:8088 -p 4040:4040 -p 4041:4041 -e JUPYTER_ENABLE_LAB=yes -v /home/gaurav.gupta/Desktop/gaurav.gupta/bits_project/:/home/jovyan/work -e CHOWN_HOME=yes -e GRANT_SUDO=yes --net=host jupyter/pyspark-notebook`

-    Get docker container IP
`docker inspect −−format '{{ .NetworkSettings.IPAddress }}' <CONTAINER_ID>`

-    Running spark app
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./work/kafka_stream_post_2.py`

