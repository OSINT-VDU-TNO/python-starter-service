import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
with open('requirements.txt') as f:
    required = f.read().splitlines()
setuptools.setup(
    name="osint-python-starter-service",
    version="2.0.1",
    author="mindpetk",
    author_email="petkeviciusm@gmail.com",
    description="Python starter service",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OSINT-VDU-TNO/python-starter-service",
    include_package_data=True,
    install_requires=required,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
