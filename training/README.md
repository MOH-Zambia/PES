# Rwandan_linkage

A set of Python scripts to demonstrate deterministic and probabilistic data linkage, using synthetic data.

## Project structure
* All related data for the project is found within `Data/`
* `m_u_values.py` calculates m and u probabilities needed for probabilistic matching, using a lookup of true match status of the records being compared. **Note: this is a simplified example as we won't know the true match status when matching PES to Census!** This will need to be ran ahead of running `Probabilistic_matching.py`
* `EM.py` contains a Python implementation of the Expectation-Maximisation algorithm for estimating m and u probabilities
* `Probabilistic_matching.py` runs Fellegi-Sunter probabilistic matching, using the M and U parameters previously calculated in `m_u_values.py`
* `rwanda_dummy_deterministic.py` gives a worked example of matching data using deterministic matchkeys
* `Levenshtein_edit_distance_example.py` is an example of the levenshtein edit distance string comparison method, used to evaluate partial agreement of string variables in matching

To run this interatively, follow this link:
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Data-Linkage/Rwandan_linkage/main)
