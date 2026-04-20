from setuptools import find_packages, setup

setup(
    name="Test_OEE",
    packages=find_packages(exclude=["Test_OEE_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
