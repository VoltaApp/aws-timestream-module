from setuptools import (
    setup,
    find_packages,
)


setup(
    name='aws_timestream_module',
    version='0.0.1',
    description='Amazon Timestream Module',
    url='git@github.com:VoltaApp/aws-timestream-module.git',
    author='Steve',
    author_email='vkhanhqui@gmail.com',
    license='unlicense',
    packages=find_packages(where='src'),
    package_dir={"": "src"},
    zip_safe=False,
    install_requires=[
        'boto3',
    ],
    python_requires='>=3.9',
    project_urls={
        'Documentation': 'https://github.com/VoltaApp/aws-timestream-module',
        'Bug Reports': 'https://github.com/VoltaApp/aws-timestream-module/issues',
        'Source Code': 'https://github.com/VoltaApp/aws-timestream-module',
    },
)
