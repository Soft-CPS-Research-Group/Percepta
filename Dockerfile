FROM python:3.12-slim
WORKDIR /percepta
COPY ./app /percepta/app
COPY requirements.txt /percepta

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD ["python", "-m", "app.start_app.entrypoint"]