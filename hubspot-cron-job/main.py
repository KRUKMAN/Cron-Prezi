from hubspot import HubSpot
import os
import sqlalchemy as db
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Authenticate with HubSpot
api_client = HubSpot(access_token=os.environ.get('HUBSPOT_ACCESS_TOKEN', 'pat-na1-73b74a13-e694-4f3d-8eff-f82e51d6aea4'))

# Set Up SQLite for Testing
DATABASE_URL = "sqlite:///mock_user_data.db"
engine = db.create_engine(DATABASE_URL)
metadata = MetaData()

# Define the 'users' table schema
users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('owner_email', String, nullable=False),
              Column('plan_type', String, nullable=False),
              Column('last_login', DateTime, nullable=False))

# Create the table
metadata.create_all(engine)

# Insert mock data into the table
connection = engine.connect()
mock_data = [
    {"owner_email": "user1@testing.com", "plan_type": "trial", "last_login": datetime(2024, 10, 1)},
    {"owner_email": "user1@qa.com", "plan_type": "pro", "last_login": datetime(2024, 9, 15)},
    {"owner_email": "vip@example.com", "plan_type": "VIP", "last_login": datetime(2024, 8, 20)}
]

connection.execute(users.insert(), mock_data)

# Function to get user data from the SQLite database
def get_user_data():
    Session = sessionmaker(bind=engine)
    session = Session()
    query = db.select(users)
    result_proxy = session.execute(query)
    # Properly convert rows to dictionaries
    result_set = [dict(row._mapping) for row in result_proxy]
    return result_set



# Function to update CRM records in HubSpot with Debugging
def update_hubspot_with_account_data(account_data):
    for account in account_data:
        license_owner = account['owner_email']  # Match account by owner email
        properties = {
            'plan_type': account['plan_type'],
            'last_login': account['last_login'].strftime('%Y-%m-%d')
        }
        try:
            # Search for the company by license_owner email
            company_search = api_client.crm.companies.search_api.do_search({
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "license_owner",  # Ensure this matches the HubSpot company property name
                                "operator": "EQ",
                                "value": license_owner
                            }
                        ]
                    }
                ],
                "properties": ["license_owner"]
            })

            # Debugging output to check search results
            print("Company search results:", company_search.results)

            # Check if the company is found and update it
            if company_search.results:
                company_id = company_search.results[0].id
                print(f"Updating company ID: {company_id} with properties: {properties}")
                api_client.crm.companies.basic_api.update(company_id, properties={'properties': properties})
                print(f"Successfully updated company ID: {company_id}")
            else:
                print(f"No matching company found for email: {license_owner}")
        except Exception as e:
            print(f"Failed to update company for email: {license_owner}, error: {e}")

# Combine functions
if __name__ == "__main__":
    user_data = get_user_data()
    if user_data:
        update_hubspot_with_account_data(user_data)