from setuptools import setup
from Cython.Build import cythonize
import numpy as np

setup(
    name='cyoptimized_OpeningRotation_funcs',
    ext_modules=cythonize("cyoptimized_OpeningRotation_funcs.pyx"),
    include_dirs=[np.get_include()] 
)