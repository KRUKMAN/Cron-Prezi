from hubspot import HubSpot
from hubspot.crm.objects import SimplePublicObjectInput
import os
import sqlalchemy as db
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import re
from config import DATABASE_URL, HUBSPOT_ACCESS_TOKEN

print(f"Connecting to database at {DATABASE_URL}")
engine = db.create_engine(DATABASE_URL)
metadata = MetaData()

# Define 'users' table
users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('owner_email', String, nullable=False),
              Column('plan_type', String, nullable=False),
              Column('last_login', DateTime, nullable=False))

# Create table if it doesn't exist
metadata.create_all(engine)
print("Users table created or already exists.")

# Connect to HubSpot API
api_client = HubSpot(access_token=HUBSPOT_ACCESS_TOKEN)
print("Connected to HubSpot API.")

# Get user data from the database
def get_user_data():
    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        result = session.execute(users.select()).fetchall()
        print(f"Fetched {len(result)} records from the users table.")
        return result
    except Exception as e:
        print(f"Error fetching user data: {e}")
        return []

# Extract company name from email domain
def extract_company_name_from_email(email):
    domain = email.split('@')[-1]
    company_name = domain.split('.')[0]
    return company_name.capitalize()

# Update or add HubSpot company with user data
def update_or_add_hubspot_company(user_data):
    for user in user_data:
        try:
            owner_email = user[1]  # Access 'owner_email'
            plan_type = user[2]
            last_login = user[3].strftime('%Y-%m-%d')
            properties = {'plan_type': plan_type, 'last_login': last_login}

            print(f"Searching for company with owner_email: {owner_email}")
            company_search = api_client.crm.companies.search_api.do_search({
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "license_owner",
                                "operator": "EQ",
                                "value": owner_email
                            }
                        ]
                    }
                ],
                "properties": ["owner_email", "plan_type", "last_login"]
            })

            if company_search.results:
                company_id = company_search.results[0].id
                existing_properties = company_search.results[0].properties

                # Check if the properties have changed before updating
                if (existing_properties.get('plan_type') != plan_type or
                        existing_properties.get('last_login') != last_login):
                    print(f"Found company ID {company_id} for owner_email: {owner_email}")
                    simple_public_object_input = SimplePublicObjectInput(properties=properties)
                    api_client.crm.companies.basic_api.update(company_id, simple_public_object_input)
                    print(f"Updated company ID {company_id} with properties: {properties}")
                else:
                    print(f"No update needed for company ID {company_id}, properties are already up to date.")
            else:
                print(f"No company found for owner_email: {owner_email}")
                # Extract company name from email domain
                company_name = extract_company_name_from_email(owner_email)
                # Add company if not found
                new_company = SimplePublicObjectInput(properties={
                    "name": company_name,
                    "license_owner": owner_email,
                    "plan_type": plan_type,
                    "last_login": last_login
                })
                api_client.crm.companies.basic_api.create(new_company)
                print(f"Created new company with owner_email: {owner_email} and name: {company_name}")
        except Exception as e:
            print(f"Error updating or adding HubSpot company for owner_email {owner_email}: {e}")


if __name__ == "__main__":
    user_data = get_user_data()
    if user_data:
        update_or_add_hubspot_company(user_data)
    else:
        print("No user data available to update in HubSpot.")
