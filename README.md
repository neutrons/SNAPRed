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
python ./src/main.py
```


## Test

```
pytest
```


## Documentation

https://snapred.readthedocs.io/en/latest/
