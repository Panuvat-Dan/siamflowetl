# __init__.py
# (empty file)

# setup.py
from setuptools import setup, find_packages

setup(
    name="siamflowetl",
    version="1.0.0",
    author="Panuvat Danvorapong",
    author_email="samdovon@gmail.com",
    description="A Python package for data ingestion and quality validation using both Pandas and Spark DataFrames.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Panuvat-Dan/siamflowetl",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "pyarrow",
        "sqlalchemy",
        "eralchemy",
        "pyspark"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
