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

Please visit the [getting started guide](https://snapred.readthedocs.io/en/latest/getting_started.html) for developer documentation.


## MCP Setup

1. if running on analysis, install npm via the following
	1. download the standalone binary to your `~/bin` folder and unzip
	2. add it to your path (change version to match your downloaded version) `export PATH="$HOME/bin/node-v24.12.0-linux-x64/bin:$PATH"
2. install copilot into your project's folder via `npm install @github/copilot --save-dev`
3. start the snapred mcp server with `pixi run python -m snapred.mcp_main`
4. run copilot cli via `npx copilot`
5. `/login` to the copilot cli
6. ask copilot to do some diffraction calibration

## Documentation

https://snapred.readthedocs.io/en/latest/
