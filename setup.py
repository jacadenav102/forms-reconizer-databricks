import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="liquidData",
    version="0.1",
    author="Antonio Cadena",
    author_email="jcadena@gmail.com",
    description="Projecto Data Liquida Contraloria General Republica",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://igerencia.com/",
    project_urls={
        "Bug Tracker": "https://igerencia.com/industrias/",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires = [
        'pyspark',
        "pandas",
        "azure-ai-formrecognizer",
        "azure.ai.textanalytics",
        "six",
        "koalas",
        "numpy",
        "pandas",
        "spacy"
        
    ],
    include_package_data = True,
)