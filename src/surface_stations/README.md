The project pipeline is defined as a sequence of three steps: 

1. Data retrieving
2. Data pre-processing
3. Model training. 

These steps are implemented as Python scripts in the `src/surface_stations` directory.

##### Make Targets

- `inmet_retrieve_ws`: Runs `retrieve_ws_inmet.py` through Make so you can inject CLI arguments via variables.

Example (downloads the INMET archive for 2019 and therefore includes all measurements from January 2019):

```bash
make inmet_retrieve_ws \
  API_TOKEN=seu_token_aqui \
  STATION_ID=A601 \
  BEGIN_YEAR=2019 \
  END_YEAR=2019
```

The output parquet file is written to `./data/ws/inmet/A601.parquet`.

#### Preprocessing

The preprocessing scripts are responsible for performing several operations on the original dataset, such as creating variables or aggregating data, which can be interesting for model training and its final result. 

#### Dataset building

These scripts will build the train, validation and test dataset from the times series produced in the previous steps. These are the datasets to be given as input to the model training step.

#### Model training and evaluation

The model generation script is responsible for performing the training and exporting the results obtained by the model after testing. 
# r2t

## Notebooks

There are several Jupyter Notebooks in the notebooks directory. They were used for initial experiments and explorarory data analisys. These notebooks are not guaranteed to run 100% correctly due to the subsequent code refactor.

Notes:
- The target uses `PYTHONPATH=src` so the script can import project modules.
- If the `atmoseer` conda environment is present on your system, Make will invoke the script using `conda run -n atmoseer python ...`. Otherwise it falls back to `python3`.
- The script accepts the same CLI flags as shown (e.g., `-e`, `-start`, `-end`, `-o`); the Make variables map to those flags.
