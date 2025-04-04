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
