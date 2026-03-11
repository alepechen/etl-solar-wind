### Description

The task is to implement an ETL (Extract, Transform, Load) client in `Python3` that will interact with several local API endpoints. It supports configurable output formats and allows data to be processed across a specified date range.

Extraction: Retrieves data from multiple API endpoints for a selected date range.

Transformation: Cleans and standardizes the retrieved data, including normalizing all timestamps to UTC.

Loading: Saves the processed data to output files in one of the supported formats.

App has 80% test coverage and uses mocks for testing.
