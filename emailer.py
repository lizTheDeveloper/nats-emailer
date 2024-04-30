## there's a NATS channel, emails.ready, that will be used to notify the emailer service that there are emails to be sent
## we'll recieve a message on emails.ready that will have the ID of the email in the database that needs to be sent
## we'll then fetch the email from the database and send it using the email service

## Database Schema:
# CREATE TABLE
#   public.emails (
#     email_id serial NOT NULL,
#     "to" character varying(255) NULL,
#     cc character varying(255) NULL,
#     bcc character varying(255) NULL,
#     "from" character varying(255) NULL,
#     body text NULL,
#     initial_prompt text NULL,
#     email_spec character varying(255) NULL,
#     audience character varying(255) NULL,
#     processed_context text NULL,
#     subject text NULL
#   );

# ALTER TABLE
#   public.emails
# ADD
#   CONSTRAINT emails_pkey PRIMARY KEY (email_id)
  
#   CREATE TABLE
#   public.email_templates (
#     template_id serial NOT NULL,
#     template_type character varying(255) NULL,
#     template_spec text NULL
#   );

# ALTER TABLE
#   public.email_templates
# ADD
#   CONSTRAINT email_templates_pkey PRIMARY KEY (template_id)
  
# CREATE TABLE
# public.contacts (
# contact_id serial NOT NULL,
# first_name character varying(255) NULL,
# last_name character varying(255) NULL,
# email character varying(255) NULL,
# relationship_context text NULL,
# private_business_info text NULL
# );

# ALTER TABLE
# public.contacts
# ADD
# CONSTRAINT contacts_pkey PRIMARY KEY (contact_id)

## we'll actually send emails using sendgrid


## import the NATS library, and async pg library
import asyncio
import asyncpg
from nats.aio.client import Client as NATS
import json
import os
import sendgrid

from sendgrid.helpers.mail import Mail

def send_email(email):
    print(email)
    sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
    # split to by , and cc by ,
    to = email["to"].split(",")
    cc = email["cc"].split(",")
    mail = Mail(
        from_email=email["from"],
        to_emails=to + cc,
        subject=email["subject"],
        plain_text_content=email["body"]
    )
    print(mail)
    response = sg.send(mail)
    return response

## now make a nats connection, listen on the emails.ready channel, and when we get a message, fetch the email from the database and send it
async def run():
    nc = NATS()
    await nc.connect(servers=["nats://0.0.0.0:4222"])
    js = nc.jetstream()
    
    async def message_handler(msg):
        email_id = msg.data.decode()
        print(f"Got message on emails.ready: {email_id}")
        print(os.environ.get("DATABASE_URL", "postgres://localhost:5432/emailer"))
        conn = await asyncpg.connect(os.environ.get("DATABASE_URL", "postgres://localhost:5432/emailer"))
        email = await conn.fetchrow('SELECT "to", cc, bcc, "from", body, subject FROM emails WHERE email_id = $1', int(email_id))
        await conn.close()
        if email:
            response = send_email(email)
            print(response)
        else:
            print(f"No email with id {email_id}")
            
        await msg.ack()

    subscription = await js.pull_subscribe("emails.ready", "emailer")
    
    print("Listening for emails to send")
    
    while True:
        messages = await subscription.fetch(timeout=None)
        for msg in messages:
            await message_handler(msg)

    
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.run_forever()
        
        
        