FROM python:3.7-alpine
LABEL Name=clientapp Version=0.0.1

WORKDIR /app

COPY Pipfile ./
COPY Pipfile.lock ./

RUN python3 -m pip install pipenv
RUN pipenv install --deploy --ignore-pipfile

COPY . .

EXPOSE 80
EXPOSE 443
EXPOSE 5555

CMD ["pipenv", "run", "python", "-u", "src/main.py"]