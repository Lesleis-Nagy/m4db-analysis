#!/usr/scripts/env python

from setuptools import setup, find_packages

setup(
    name="m4db_analysis",
    version="0.0.0",
    packages=find_packages(
        where="lib",
        include="m4db_analysis/*"
    ),
    package_dir={"": "lib"},
    install_requires=[
        "typer",
        "rich",
        "matplotlib",
        "numpy",
        "pandas",
        "mpmath",
        "sklearn",
        "psycopg2"
    ],
    entry_points="""
    [console_scripts]
    m4db-analysis=m4db_analysis.entry_point:main
    """
)
