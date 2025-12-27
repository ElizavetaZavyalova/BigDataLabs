FROM apache/spark:3.5.3

WORKDIR /app
COPY build/libs/*.jar app.jar
COPY build/libs/lib lib/
COPY app/data /data

ENV PATH="/opt/spark/bin:$PATH"

CMD ["spark-submit", "--class", "com.example.RunApp", "--jars", "lib/*", "app.jar"]
#spark-submit --class com.example.RunApp --jars  "lib/*" app.jar