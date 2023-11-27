"""
The global settings singleton
"""

import warnings

class Globals():

    def __init__(self):
        self._experimental_vector_search = False

    @property
    def experimentalVectorSearch(self):
        return self._experimental_vector_search

    def enableExperimentalVectorSearch(self):
        warnings.warn(
            (
                'The "experimentalVectorSearch" flag is being deprecated: '
                'please remove references to it from your code.'
            ),
            DeprecationWarning,
        )
        self._experimental_vector_search = True

globals = Globals()
