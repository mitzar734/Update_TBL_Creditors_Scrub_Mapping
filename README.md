# Google Sheets to Snowflake Data Integration

## Description

This Python script extracts data from a Google Sheet and uploads it into a Snowflake table. The Google Sheet contains creditor information, and the data is processed and cleaned before being loaded into the Snowflake database.

## Prerequisites

To run this script, you will need the following:

- Python 3.x
- Required Python libraries:
  - `pandas`
  - `gspread`
  - `snowflake-connector-python`
  - `google-auth`
- Snowflake account credentials
- Google service account credentials for accessing Google Sheets
- Snowflake table to upload data

## Setup

1. **Install required libraries:**
   Install the necessary Python libraries by running the following command:

   ```bash
   pip install pandas gspread snowflake-connector-python google-auth
