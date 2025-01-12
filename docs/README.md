## Getting Started

## Overview
This project is a FastAPI application developed by **Michael Liav** from the Platform Team. It serves as a web application with a focus on providing a robust API and a user-friendly interface.


# Getting started

## Prerequisites
- Python 3.12 or 3.13
- `pip` package manager
# Project tree
```bash
project/
│ 
├───.harness #Automation directory **Do not Delete**
│
├───assets
│   └───swagger-ui 
│
├───docs #Documentation directory
│
├───middlewares
│   └──whitelist_middleware.py #Whitelist middeware
│
├───platform # Contains all file related to infra and harness **Do not Delete**
│   
├───routes
│   └───api.py #Routes file contains all health checks
│ 
└───tests #Test directory
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

## Running The Project
```bash
uvicorn main:app --port 7860
```

## Running Coverage
```bash
pytest --junitxml=report.xml --cov=. --cov-report=term ./tests
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
site_name: migdal.platform.templates.java17.lib //Project Name

nav:
  - Home: README.md //Update to relevant project docs you have added
  // Example
  - Getting started: start.md
    
plugins:
  - techdocs-core
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