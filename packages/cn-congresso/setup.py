#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for congresso.

    This file was generated with PyScaffold 2.5.7, a tool that easily
    puts up a scaffold for your new Python project. Learn more under:
    http://pyscaffold.readthedocs.org/
"""

from setuptools import setup


setup(name='Congresso',
      version='0.1.3',
      description='Low Level Python Client of Brazilian Camara e Senado Federal API',
      author='Joao Carabetta',
      author_email='joao.carabetta@gmail.com',
      packages=['congresso', 'congresso/camara'],
      license='APACHE',
      url= 'https://github.com/CongressoEmNumeros/congresso'
     )

if __name__ == "__main__":
    setup()
