FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir streamlit==1.31.0 pandas==2.1.4 plotly==5.18.0

COPY dashboard.py .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
