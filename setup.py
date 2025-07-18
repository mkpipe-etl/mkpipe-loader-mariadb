from setuptools import setup, find_packages

setup(
    name='mkpipe-loader-mariadb',
    version='0.2.2',
    license='Apache License 2.0',
    packages=find_packages(exclude=['tests', 'scripts', 'deploy', 'install_jars.py']),
    install_requires=['mkpipe'],
    include_package_data=True,
    entry_points={
        'mkpipe.loaders': [
            'mariadb = mkpipe_loader_mariadb:MariadbLoader',
        ],
    },
    description='MariaDB loader for mkpipe.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
    ],
    python_requires='>=3.8',
)
