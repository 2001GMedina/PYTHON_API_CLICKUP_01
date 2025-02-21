ClickUp API to Oracle:
This project retrieves data from the ClickUp API using the GET method with the requests library, processes the information using pandas, and applies insertion logic with the holidays and datetime libraries. The processed data is then inserted into an Oracle table using the pyodbc library. This project is useful for automating the ingestion of ClickUp data based on date controls and integrating it into a relational database..

#Configuration

Create a .env file in the root directory of the project with the following environment variables:

DSN=YOUR_BD_DSN
USER=YOUR_BD_USER
PASSWORD=YOUR_BD_PASSWORD
CLICKUP_KEY=YOUR_CLICKUP_API_KEY

Ensure that the credentials for both the ClickUp API and the Oracle database are correctly configured.

#Usage:
Run the script to collect data from the ClickUp API and insert it into the Oracle table:

python main.py

The script collects data from the API, processes it, and inserts it into the specified Oracle table.
