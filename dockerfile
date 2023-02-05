FROM telegraf:1.25.0

COPY services/telegraf/rabbit.conf /etc/telegraf/telegraf.conf