FROM python:3.11

# Install pre-requirements
RUN pip install pip~=23.3.2 setuptools~=69.0.3

COPY requirements-full.txt ./
RUN pip install -r requirements-full.txt
COPY ./local_app /src/local_app
COPY ./data/external /external
COPY ./data/processed /processed
WORKDIR /src/local_app
#CMD ["flask", "run", "--host", "0.0.0.0"]
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:create_app()"]
