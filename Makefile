install:
	pip install --upgrade pip
	pip install -r requirements.txt

setup:
	pyenv virtualenv 3.13.2 surfe
	pyenv activate surfe
	make install


