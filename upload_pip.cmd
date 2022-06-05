:: pip3 install --user --upgrade twine

:: pip3 install --user --upgrade setuptools wheel

python setup.py sdist bdist_wheel

:: twine upload --repository-url https://test.pypi.org/legacy/ dist/*

twine upload dist/*