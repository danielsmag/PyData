# Python Library Template Documentation
## Getting Started

## Overview
This project is example library developed by **Michael Liav** from the Platform Team. It releases a simple package for python which recives a name as input and prints Hello {name}


# Getting started

## Prerequisites
- Python 3.12 or 3.13
- `pip` package manager
# Project tree
```bash
project/
│ 
├─── .harness #Automation directory **Do not Delete**
│ 
├─── platform #Contains the library harness-info.yaml **Do not Delete**
│ 
├── src/
│   └── template/ #Contains all the library files
│       
├── tests/ #Test directory
│   
├── pyproject.toml #Project toml for releasing package
```
## Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd your_project
```

## Installing The Requirements 
```bash 
pip install -r requirements.txt --index-url https://jfrog-platform.shared.migdal-group.co.il/artifactory/api/pypi/pythonvirtual/simple
```

## Running Coverage
```bash
pytest --junitxml=report.xml --cov=. --cov-report=xml --cov-report=html ./tests
```
## Running Quality Checks
## Isort
isort is a Python utility that automatically sorts and organizes your imports according to PEP 8 conventions. It groups imports into sections:
- Standard library imports
- Third-party imports
- Local application imports
```bash
isort .
```
### Black
Black is the uncompromising Python code formatter. It formats your code in a consistent style by enforcing a strict subset of PEP 8. Key benefits:
- Deterministic output: The same input will always produce the same output
- Zero configuration: Just run it and get consistent formatting
- Auto-formats code on save (when configured with your IDE)
```bash 
black .
```
### Pylint
Pylint is a static code analyzer that looks for programming errors, helps enforce coding standards, and can make suggestions about how to refactor your code. It checks for:
- Coding standards compliance
- Error detection
- Refactoring help
- Duplicate code detection
- Code complexity checks
```bash 
pytlint .
```
### Running all quality checks
```bash
isort .
black .
pylint .
```
# Configure Project Properties
Update mkdocs.yaml
```yaml
site_name: migdal.platform.templates.lib.python //Project Name

nav:
  - Home: README.md 
  - CHANGELOG: CHANGELOG.md
  //Update to relevant project docs you have added
  // Example  
  - example: example.md
    
plugins:
  - techdocs-core
```
Update setup.py
```py
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="template", # Change to your library name
    version="0.1.0", # Change version for release
    author="Michael Liav", # Change to your name
    author_email="liavm@migdal.co.il", # Change to your email
    description="A small example package", # Change description for the package
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://migdal.harness.io/ng/account/NmRjYjU0MzgtMTQ2MS00ZW/module/code/orgs/Platform/projects/templates/repos/migdal.platform.templates.lib.python", #Change to your project url
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.12.2",
    install_requires=[
        # Add your dependencies here
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
        "Homepage": "https://migdal.harness.io/ng/account/NmRjYjU0MzgtMTQ2MS00ZW/module/code/orgs/Platform/projects/templates/repos/migdal.platform.templates.lib.python", #Change to your project url
    },
)
```
# Development Guidelines
## Code Style
* Follow Python standard naming conventions
* Use meaningful names for classes and methods
* Include comments for public APIs
* Follow the team's coding standards

## Testing
* Write unit tests for new functionality
* Place tests in `tests` directory
* Aim for high test coverage

## Version Control
* Use meaningful commit messages
* Create feature branches for new development
* Follow Git flow branching strategy
* Tag releases with version numbers

## Best Practices
### Dependency Management
* Keep dependencies up to date
* Use version ranges carefully
* Document any specific version requirements

### Testing Best Practices
* Write unit tests for new code
* Include integration tests where necessary
* Test edge cases
* Use meaningful test names

### Documentation
* Document public APIs
* Include usage examples
* Keep README up to date
* Document breaking changes
* Place all relevant documentation in docs directory