from setuptools import setup, find_packages

setup(
    name='cloud-dataframe',
    version='0.1.0',
    description='A Python DSL for type-safe dataframe operations that generates SQL for database execution',
    author='Neema Raphael',
    author_email='neema.raphael@gs.com',
    packages=find_packages(),
    install_requires=[
        'typing-extensions',
        'duckdb',
    ],
    python_requires='>=3.8',
)
