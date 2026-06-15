FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto/ ./proto/
COPY server/ ./server/
RUN python3 -m grpc_tools.protoc -I proto \
    --python_out=server/grpc \
    --grpc_python_out=server/grpc \
    proto/sensor.proto && \
    sed -i 's/^import sensor_pb2/import server.grpc.sensor_pb2/' \
    server/grpc/sensor_pb2_grpc.py
EXPOSE 50051 8007
CMD ["sh", "-c", "python3 server/grpc/main.py & uvicorn server.grpc.health_server:app --host 0.0.0.0 --port 8007"]
