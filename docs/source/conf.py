# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))
sys.path.insert(0, os.path.abspath('../../src'))
sys.path.insert(0, os.path.abspath('.'))

project = 'glue_sdk'
copyright = '2025, Daniel Smagarinsky'
author = 'Daniel Smagarinsky'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.autosectionlabel',
    'sphinxcontrib.mermaid',
    'sphinx_autodoc_typehints',
    'sphinx.ext.viewcode',
    'myst_parser',
    
]
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}
templates_path = ['_templates']
exclude_patterns = []

html_search_language = 'en'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = 'alabaster'
# html_theme = 'furo'
html_theme='sphinx_rtd_theme'


html_static_path = ['_static']
master_doc = 'index'
autodoc_typehints = 'description'
autosectionlabel_prefix_document = True