from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="template",
    version="0.1.0",
    author="Michael Liav",
    author_email="liavm@migdal.co.il",
    description="A small example package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://migdal.harness.io/ng/account/NmRjYjU0MzgtMTQ2MS00ZW/module/code/orgs/Platform/projects/templates/repos/migdal.platform.templates.lib.python",
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
        "Homepage": "https://migdal.harness.io/ng/account/NmRjYjU0MzgtMTQ2MS00ZW/module/code/orgs/Platform/projects/templates/repos/migdal.platform.templates.lib.python",
    },
)
