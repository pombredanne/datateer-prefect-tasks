from setuptools import setup

setup(name='datateer_prefect_tasks',
      version='0.1.13',
      description='Prefect Pipeline Tasks by Datateer',
      url='http://github.com/aroder/datateer-pipeline-tasks',
      author='Datateer',
      author_email='hello@datateer.com',
      license='None',
      packages=['datateer.tasks'],
      install_requires=[
          'prefect==0.7.2'
      ],

      zip_safe=False)