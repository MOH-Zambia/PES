# Data Linkage Training Scripts

This directory contains scripts to help explain record linkage concepts such as edit distance and probabilistic matching.

**Any required data for the training is found within `Data/`.**

## Training script contents

### Creating and using matchkeys
* `deterministic_example.py` gives a worked example of matching data using deterministic matchkeys
* `Levenshtein_edit_distance_example.py` is an example of the levenshtein edit distance string comparison method, used to evaluate partial agreement of string variables in matching
* `Alphaname.py` contains the function to create alphanames which is a common match key for names 


### Probability matching
* `m_u_values.py` calculates m and u probabilities needed for probabilistic matching, using a lookup of true match status of the records being compared. **Note: this is a simplified example as we won't know the true match status when matching PES to Census!** This will need to be ran ahead of running `Probabilistic_matching.py`
* `EM.py` contains a Python implementation of the Expectation-Maximisation algorithm for estimating m and u probabilities
* `Probabilistic_matching.py` runs Fellegi-Sunter probabilistic matching, using the M and U parameters previously calculated in `m_u_values.py`


<!--
To run this interatively, follow this link:
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Data-Linkage/Rwandan_linkage/main)
-->