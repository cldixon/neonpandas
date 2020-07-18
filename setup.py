from setuptools import setup, find_packages

setup(name="neonpandas",
      version="0.1",
      url="https://github.com/cldixon/neonpandas",
      license="MIT",
      author="CL Dixon",
      author_email="cl_dixon@icloud.com",
      description="A Pandas-Centric Interface to Neo4j",
      packages=find_packages(exclude=['test']),
      long_description=open('README.md').read(),
      zip_safe=False
)
 
'''
setup(name='pathology',
      version='0.1',
      url='https://github.com/the-gigi/pathology',
      license='MIT',
      author='Gigi Sayfan',
      author_email='the.gigi@gmail.com',
      description='Add static script_dir() method to Path',
      packages=find_packages(exclude=['tests']),
      long_description=open('README.md').read(),
      zip_safe=False)
'''