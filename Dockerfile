FROM python:3.9-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /percepta
COPY ./app /percepta/app
COPY requirements.txt /percepta

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD ["python", "-m", "app.start_app.entrypoint"]