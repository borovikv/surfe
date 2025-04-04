install:
	pip install --upgrade pip
	pip install -r requirements.txt

setup:
	pyenv virtualenv 3.12.9 surfe
	pyenv activate surfe
	make install


build:
	docker compose build
	docker compose up airflow-init


up:
	docker compose up


clean:
	find . -name "*.pyc" -type f -delete


tests:
	make clean
	PYTHONPATH=./dags/etl \
	pytest -s -vv --cov=. --testdox --cov-report term-missing


docker-test:
	docker compose exec airflow-webserver \
		pytest -s -vv --cov=. --testdox --cov-report term-missing -W ignore::DeprecationWarning

