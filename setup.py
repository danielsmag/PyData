from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="glue_sdk",
    version="0.1.0",
    author="Daniel Smagarinsjy",
    author_email="danielsm@migdal.co.il",
    description="custom sdk for glue/spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://migdal.harness.io/ng/account/NmRjYjU0MzgtMTQ2MS00ZW/module/code/orgs/Platform/projects/templates/repos/migdal.platform.templates.lib.python",
    package_dir={"": "src"},
    packages=find_packages(where="src",exclude=["main.py"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10.0",
    install_requires=[
        'psycopg2-binary>=2.9.10',
        'pydantic>=2.10.4',
        'pydantic-settings>=2.7.0',
        'dependency-injector>=4.44.0',
        'opensearch-py>=2.7.1',
        'redis>=5.2.1',
        'PyYaml>=6.0.2',
        'boto3>=1.36.9'
    ],
    extras_require={
        'dev': [
            'pytest>=7.0',
            'pytest-cov>=3.0',
            'pylint>=2.17.0',
        ],
        'test': [
            'pytest>=7.0',
            'pytest-cov>=3.0',
        ],
    },
    project_urls={
        "Homepage": "https://migdal.harness.io/ng/account/NmRjYjU0MzgtMTQ2MS00ZW/module/code/orgs/Platform/projects/templates/repos/migdal.platform.templates.lib.python",
    },
)
