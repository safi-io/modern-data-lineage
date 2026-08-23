FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root

# Create a real home directory
RUN mkdir -p /home/spark \
    && chown -R spark:spark /home/spark

# Install Jupyter and Pyspark
RUN pip install --no-cache-dir jupyter
RUN pip install --no-cache-dir pyspark


# Creating some required writable directories
RUN mkdir -p \
    /home/spark/.ivy2 \
    /home/spark/.ivy2.5.2 \
    /home/spark/.local/share/jupyter/runtime \
    /opt/notebooks \
    && chown -R spark:spark /home/spark /opt/notebooks

ENV HOME=/home/spark
ENV USER=spark
ENV JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime

USER spark

WORKDIR /opt/notebooks

CMD ["jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser"]