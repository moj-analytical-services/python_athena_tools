from setuptools import setup

setup(name='python_athena_tools',
      version='0.0.1',
      description='A wrapper around Athena to load query results into Python memory as fast as possible',
      url='https://github.com/moj-analytical-services/python_athena_tools',
      author='Robin Linacre, Karik Isichei',
      author_email='robinlinacre@hotmail.com',
      license='MIT',
      packages=['python_athena_tools'],
      setup_requires=['pandas', 'pyarrow', 's3fs', 'boto3'],
      test_requires=["pylint", "coverage", "codecov"],
      zip_safe=False)