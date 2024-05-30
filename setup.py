from setuptools import setup, find_packages

setup(
    name='bwc_compression',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pymeos',
        'numpy',
        'pandas',
        'pytest',
        # Add other dependencies here
    ],
    # entry_points={
    #     'console_scripts': [
    #         # Define any command-line scripts here
    #     ],
    # },
    author='Gilles Dejaegere',
    author_email='gilles.dejaegere@ulb.be',
    description='Comparison of algortihms for compression of trajectories under bandwidth constraints.',
    url='https://github.com/gdejaege/bwc',
    classifiers=[
        'Programming Language :: Python :: 3',
        # 'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
