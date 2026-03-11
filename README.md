## Technical Task
### Description

The task is to implement an ETL (Extract, Transform, Load) client in `Python3` that will interact with a local API data source.

Preferred Python version >= 3.8 (https://devguide.python.org/versions/)

You may only use appropriately licensed and open source 3rd party libraries when necessary.


### Task
Write an ETL client that will:
1. Extract data only from latest week from both: `Solar` and `Wind` endpoints.
2. Transform naive timestamps from the data source to a timezone aware `utc` format.
3. Ensure column naming and column type are adhering proper naming strategy and types.
4. Finally load the data to an `/output` directory, using a file format of your choosing. Keeping in mind directory and filename structure/convention.

Do not change the `/api_data_source`

### Assessment
The task will be assessed on design, implementation, functionality, documentation, and testability.

Treat this as a production release.


### Submission
The code should	be pushed to version control repository that can be easily retrieved and ran locally.


### Getting Started

1. Setup a virtual or package manager environment (`pipenv`, `conda`, etc.)
2. Install requirements into your environment: `pip install -r requirements.txt`
3. Run the API data source: `python -m uvicorn api_data_source.main:app --reload`
4. Open and view the API documentation: `http://127.0.0.1:8000/docs` or `http://localhost:8000/docs`


Good luck!
