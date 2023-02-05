FROM python:3.9

RUN pip install paho-mqtt
RUN pip install numpy
RUN pip install pandas

COPY . /app
WORKDIR /app
CMD ["python", "mqtt_client.py"]
