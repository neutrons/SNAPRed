<!-- Badges -->

[![Build Status](https://github.com/neutrons/SNAPRed/actions/workflows/actions.yml/badge.svg?branch=next)](https://github.com/neutrons/SNAPRed/actions/workflows/actions.yml?query=branch?next)
[![Documentation Status](https://readthedocs.org/projects/snapred/badge/?version=latest)](https://snapred.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/neutrons/SNAPRed/branch/next/graph/badge.svg)](https://codecov.io/gh/neutrons/SNAPRed/tree/next)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/7193/badge)](https://bestpractices.coreinfrastructure.org/projects/7193)

<!-- End Badges -->


# SNAPRed
## Description

A desktop application for Lifecycle Managment of data collected from the SNAP instrument.
Planned to include: Reduction, Calibration, Data Exploration, and Diagnosis

## Build
### Conda
Create your conda envirionment:
```
conda env create --file environment.yml
activate SNAPRed
```

Update if its been a while:
```
activate SNAPRed
conda env update --file environment.yml  --prune
```


## Run

```
cd src
python -m snapred
```


## Test

```
pytest
```


## Documentation

https://snapred.readthedocs.io/en/latest/
