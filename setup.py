from setuptools import setup, find_packages

setup(
    name="etl-agent",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'anthropic',
        'google-cloud-aiplatform',
        'google-cloud-storage',
        'pandas',
        'openpyxl',
        'python-dotenv'
    ],
    python_requires='>=3.8'
)
