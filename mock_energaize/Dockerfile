FROM python:3.12-slim
WORKDIR /mock_energaize
RUN pip install --no-cache-dir "aiohttp~=3.13.3"
COPY server.py .
EXPOSE 8000
CMD ["python", "-u", "server.py"]
