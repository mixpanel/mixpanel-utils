# from distutils.core import setup
from setuptools import setup

setup(
    name='mixpanel-utils',
    packages=['mixpanel_utils'],
    version='2.2.5',
    description='Utilities-only module for exporting and importing data into Mixpanel. NOT for server-side app tracking.',
    long_description='A utilities-only module for exporting and importing data into Mixpanel via their APIs. This is NOT for server-side app tracking.',
    author='Jared McFarland',
    author_email='jared@mixpanel.com',
    url='https://github.com/mixpanel/mixpanel-utils',
    python_requires='>=3'
)
