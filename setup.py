import setuptools
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(os.path.join(os.path.dirname(__file__), "requirements.txt"), "r") as f:
    requirements = f.read().split("\n")

setuptools.setup(
    name="feliz_db",
    version="0.0.4",
    author="Vincent Wu, Linga Chen, Brian Yin",
    author_email="zenith3092@gmail.com",
    description="A framework designed to assist in using databases and compatible with feliz framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zenith3092/feliz_db",
    license="MIT",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)