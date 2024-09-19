import ast
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import shapely

import openeo
from openeo.extra.job_management import MultiBackendJobManager, JobDatabaseInterface


class OpenEOProcessJobManager():
    """
    EXPERIMENTAL
    A tool to manage multiple jobs that execute a specific (user defined) openEO process.

    This interface is under development, thus the API may be subject to change in the future.

    The parameters of the provided openEO process will be parsed, and automatically matched to the union of the fixed
    parameters and the parameters available in the job database.

    This allows the tool to automatically create correct openEO jobs, and track their status.
    """
    
    def __init__(self, process_id:str, process_namespace:str, fixed_parameters:dict, job_db:JobDatabaseInterface, job_options:dict=None):
        """
        Create a new job manager

        :param process_id: The id of the user defined process
        :param process_namespace: The namespace of the user defined process
        :param fixed_parameters: A dictionary with parameters that are the same for all jobs.
        :param job_db: A job database interface to store the jobs, can be empty
        :param job_options: job options to be used.

        """
        super().__init__()
        self.largescale_process = None
        self._job_options = job_options
        self.fixed_parameters = fixed_parameters
        self.process_namespace = process_namespace
        self.process_id = process_id

        self._job_manager = MultiBackendJobManager()
        self._job_db = job_db

        self._parse_process_definition()

    def _parse_process_definition(self):
        self._process_metadata = requests.get(self.process_namespace).json()

    @property
    def job_options(self):
        return self._job_options

    @job_options.setter
    def job_options(self, value):
        self._job_options = value

    def process_parameters(self) -> list[dict]:
        return self._process_metadata["parameters"]

    def process_parameter_schema(self, name:str) -> Optional[dict]:
        return {p["name"]:p.get("schema",None) for p in self.process_parameters()}.get(name, None)


    def add_jobs(self, jobs_dataframe:pd.DataFrame):
        """
        Add jobs to the job manager.

        Column names of the dataframe have to match with process parameters.

        Extra columns names that will be used:

        - `title` : Title of the job
        - `description` : Description of the

        Reserved column names, that are added and used by the job manager:

        - 'status': Will be set to 'not_started'.
        - 'id' The job id
        - start_time
        - running_start_time
        - cpu
        - memory
        - duration
        - backend_name

        Additional column names are allowed, and will be stored in the job database 'as-is'.

        :param jobs_dataframe: A pandas dataframe with the jobs to be added.

        """

        df = self._job_manager._normalize_df(jobs_dataframe)

        def normalize_fixed_param_value(name, value):
            if isinstance(value, list) or isinstance(value, tuple):
                return len(df) * [value]
            else:
                return value

        new_columns = {
            col: normalize_fixed_param_value(col, val) for (col, val) in self.fixed_parameters.items() if
            col not in df.columns
        }
        new_columns["process_id"] = self.process_id
        new_columns["process_namespace"] = self.process_namespace
        print(new_columns)
        df = df.assign(**new_columns)

        geojson_params = [p["name"] for p in self.process_parameters() if
                          p.get("schema", {}).get("subtype", "") == "geojson"]

        if len(geojson_params) == 1:
            # TODO: this is very limited, expand to include more complex cases:
            # - bbox instead of json
            if geojson_params[0] not in df.columns:
                df.rename_geometry(geojson_params[0], inplace=True)
        elif len(geojson_params) > 1:
            for p in geojson_params:
                if p not in df.columns:
                    raise ValueError(
                        f"Multiple geojson parameters, but not all are in the dataframe. Missing column: {p}, available columns: {df.columns}")


        self._job_db.persist(df)



    def start_job_thread(self):
        """
        Start running the jobs in a separate thread, returns afterwards.
        """

        if not self._job_db.exists():
            raise ValueError(
                f"The job database does not yet exist. Either add jobs to the manager, or load an existing job database.")

        process_parameter_names = [p["name"] for p in self.process_parameters()]

        def start_job(
                row: pd.Series,
                connection: openeo.Connection,
                **kwargs
        ) -> openeo.BatchJob:

            def normalize_param_value(name, value):
                schema = self.process_parameter_schema(name)
                if isinstance(value, str) and schema.get("type","") == "array":
                    return ast.literal_eval( value )
                elif isinstance(value, str) and schema.get("subtype","") == "geojson":
                    #this is a side effect of using csv + renaming geometry column
                    return shapely.geometry.mapping(shapely.wkt.loads(value))
                else:
                    return value

            parameters = {k: normalize_param_value(k,row[k]) for k in process_parameter_names }

            cube = connection.datacube_from_process(row.process_id,row.process_namespace, **parameters)

            title = row.get("title", f"Subjob {row.process_id} - {str(parameters)}")
            description = row.get("description", f"Subjob {row.process_id} - {str(parameters)}")
            return cube.create_job(title=title, description=description)


        self._job_manager.start_job_thread(start_job=start_job)


    def stop_job_thread(self):
        """
        Stop running jobs.
        """
        self._job_manager.stop_job_thread()
