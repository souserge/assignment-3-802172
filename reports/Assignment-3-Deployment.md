# This your assignment deployment report

The deployment of the app is very straightforward:

1. run `docker-compose up -d` in the root directory of the project.
2. run `docker-compose logs` to check if all services are running correctly.
3. The RabbitMQ monitoring and management tool should be accessible on `http://localhost:15672`

Additionally, a JupyterLab data exploration environment can be run. To do so:

1. go to `data/` directory (`cd data`)
2. run `docker-compose up -d`
3. check `docker-compose logs` is all runs properly, and copy the access token
4. go to `https://localhost:7777` and connect with the access token