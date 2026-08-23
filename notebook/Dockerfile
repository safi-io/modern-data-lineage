FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root

RUN mkdir -p /home/spark \
    && chown -R spark:spark /home/spark

RUN pip install --no-cache-dir jupyter

ENV HOME=/home/spark
ENV JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime

RUN mkdir -p /home/spark/.local/share/jupyter/runtime \
    && chown -R spark:spark /home/spark

USER spark

WORKDIR /opt/notebooks

CMD ["jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser"]