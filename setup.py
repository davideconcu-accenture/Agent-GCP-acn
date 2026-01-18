from setuptools import setup, find_packages

setup(
    name="etl-agent",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'flask>=3.0.0',
        'gunicorn>=21.2.0',
        'anthropic>=0.39.0',
        'google-cloud-aiplatform>=1.60.0',
        'google-cloud-storage>=2.18.0',
        'pandas>=2.2.0',
        'openpyxl>=3.1.0',
        'python-dotenv>=1.0.0'
    ],
    python_requires='>=3.11'
)
