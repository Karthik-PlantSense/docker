FROM bitnami/zookeeper:3.8

USER root
# RUN commands
# USER 1001

RUN apt-get update && apt-get install net-tools -y

USER 1001

ENTRYPOINT ["/opt/bitnami/scripts/zookeeper/entrypoint.sh"]

CMD ["/opt/bitnami/scripts/zookeeper/run.sh"]