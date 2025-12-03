FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
# COPY ../.env /app/
EXPOSE 1024

CMD ["python", "-m", "app.run"]