
FROM python:latest
WORKDIR /app
ADD ./requirements.txt /app
ADD ./script.py /app
RUN pip install -r requirements.txt
CMD [ "python", "-u", "/app/script.py"]
