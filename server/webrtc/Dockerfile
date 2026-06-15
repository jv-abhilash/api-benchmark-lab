FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto/ ./proto/
COPY server/ ./server/
EXPOSE 8006
CMD ["uvicorn", "server.webrtc.main:app", "--host", "0.0.0.0", "--port", "8006"]
