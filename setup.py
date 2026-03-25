from setuptools import setup, find_packages

setup(
    name='mixpanel-utils',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    version='3.1.0',
    description='Utilities-only module for exporting and importing data into Mixpanel. NOT for server-side app tracking.',
    long_description='A utilities-only module for exporting and importing data into Mixpanel via their APIs. This is NOT for server-side app tracking. Service Account authentication required.',
    author='Jared McFarland',
    author_email='jared@mixpanel.com',
    url='https://github.com/mixpanel/mixpanel-utils',
    python_requires='>=3',
    extras_require={
        'streaming': [
            'httpx>=0.27',
            'click>=8.0',
            'pyarrow>=14.0',
            'mmh3>=4.0',
            'python-dateutil>=2.8',
            'aiofiles>=23.0',
        ],
        'streaming-gcs': ['gcsfs>=2024.1'],
        'streaming-s3': ['aiobotocore>=2.9'],
        'streaming-all': [
            'httpx>=0.27',
            'click>=8.0',
            'pyarrow>=14.0',
            'mmh3>=4.0',
            'python-dateutil>=2.8',
            'aiofiles>=23.0',
            'gcsfs>=2024.1',
            'aiobotocore>=2.9',
        ],
    },
    entry_points={
        'console_scripts': [
            'mixpanel-utils=mixpanel_utils.streaming.cli:main',
        ],
    },
)
