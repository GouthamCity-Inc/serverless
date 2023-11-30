import json
import logging
import requests
import zipfile
import os
from google.cloud import storage
from google.oauth2 import service_account
import boto3
import base64
import smtplib
from email.mime.text import MIMEText


google_base64_creds = os.environ['GOOGLE_CREDENTIALS']
bucket_name = os.environ['BUCKET_NAME']
smtp_host = os.environ['SMTP_HOST']
smtp_port = os.environ['SMTP_PORT']
smtp_username = os.environ['SMTP_USERNAME']
smtp_password = os.environ['SMTP_PASSWORD']
sender_email = os.environ['SENDER_EMAIL']
dynamodb_table = os.environ['DYNAMODB_TABLE']
print("dynamodb_table: ", dynamodb_table)

google_creds_decoded = base64.b64decode(google_base64_creds).decode('utf-8')
google_creds_json = json.loads(google_creds_decoded)
credentials = service_account.Credentials.from_service_account_info(google_creds_json)

class SNSMessage:
    def __init__(self, id, submissionID, status, message, url, email, attempt, timestamp):
        self.id = id
        self.url = url
        self.email = email
        self.attempt = attempt
        self.submissionID = submissionID
        self.status = status
        self.message = message
        self.timestamp = timestamp

def download_from_url(url, destination_folder, file_name):
    response = requests.get(url)
    if response.status_code == 200:
        # Save the downloaded zip file
        zip_file_path = os.path.join(destination_folder, file_name)
        with open(zip_file_path, 'wb') as zip_file:
            zip_file.write(response.content)

        # Extract the contents of the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(destination_folder)

        return True
    else:
        return False
    
# def send_email_test():
#     msg = MIMEText('Testing some Mailgun awesomness')
#     msg['Subject'] = "Hello!"
#     msg['From']    = "goutham@gouthamhusky.me"
#     msg['To']      = "kanahasabai.g@northeastern.edu"

#     s = smtplib.SMTP('smtp.mailgun.org', 587)

#     s.login('postmaster@gouthamhusky.me	', 'mailgungoutham')
#     s.sendmail(msg['From'], msg['To'], msg.as_string())
#     s.quit()


def send_email(message_pojo, receiver_email, file_name):

    email = message_pojo.email
    user_name = email.split('@')[0]
    status = message_pojo.status
    message = message_pojo.message
    url = message_pojo.url
    attempt = message_pojo.attempt
    submission_id = message_pojo.submissionID
    assignment_id = message_pojo.id

    print("email: ", email)
    print("user_name: ", user_name)
    print("status: ", status)
    print("message: ", message)
    print("url: ", url)
    print("attempt: ", attempt)
    print("submission_id: ", submission_id)
    print("assignment_id: ", assignment_id)

    if message_pojo.status == "SUCCESS":
        greeting = f"Hi {user_name}! Your submission was successful!"
    elif message_pojo.status == "ERROR":
        greeting = f"Hi {user_name}! There was an issue with your submission."
    else:
        greeting = "Hi! We have an update about your submission."

    if message_pojo.status == "SUCCESS":
        file_path = f"/{user_name}/{assignment_id}/{file_name}"
        message_body = "{}\n\n" \
               "Your submission with ID {} has the following details:\n" \
               "Status - {}\n" \
               "File Path - {}\n" \
               "Attempt  - {}\n\n\n" \
               "Regards,\n" \
               "MGEN Grading Council"
        message_body = message_body.format(greeting, assignment_id, status, file_path, attempt)

    else:
        message_body = "{}\n\n" \
               "Your submission with ID {} has the following details:\n" \
               " Status - {}\n" \
               " Message - {}\n\n\n" \
               "Regards,\n" \
               "MGEN Grading Council"
        message_body = message_body.format(greeting, assignment_id, status, message)

    subject = f"Update on Your Submission (ID: {assignment_id})"

    print("message body: ", message_body)
    msg = MIMEText(message_body)
    msg['Subject'] = subject
    msg['From']    = sender_email
    msg['To']      = receiver_email

    s = smtplib.SMTP(smtp_host, smtp_port)
    s.login(smtp_username, smtp_password)
    s.sendmail(msg['From'], msg['To'], msg.as_string())
    s.quit()

    
def upload_to_gcs(source_file_path, file_name, assignment_id, username):
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    destination_blob_name = f'{username}/{assignment_id}/{file_name}'
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)

def insert_data_into_dynamodb(table_name, data):
    try:
        dynamodb = boto3.resource('dynamodb')
        print("In insert_data_into_dynamodb()")
        table = dynamodb.Table(table_name)
        print("table: ", table)
        response = table.put_item(Item=data)
        print("response: ", response)
        return {
            'statusCode': 200,
            'body': json.dumps('Data inserted into DynamoDB successfully!'),
            'response': response
        }
    except Exception as e:
        error_message = f"Error inserting data into DynamoDB: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps(error_message)
        }
 
def update_dynamodb(user_email, assignment_id, submission_url, full_path, timestamp):
    partition_key = f"{user_email}#{assignment_id}#{timestamp}"
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table)
    response = table.put_item(
        Item={
            'id': partition_key,
            'assignment-Id': assignment_id,
            'submission-url': submission_url,
            'file-path': full_path,
            'timestamp':  timestamp
        }
    )
    print("response: ", response)


def lambda_handler(event, context):
    try: 
        # print the environment variable: GOOGLE_APPLICATION_CREDENTIALS
        print("Received event: " + json.dumps(event, indent=2))
        message = event['Records'][0]['Sns']['Message']
        message_pojo = json.loads(message, object_hook=lambda d: SNSMessage(**d))
        print("From SNS: ", message)
        print("POJO url: " + message_pojo.url)
        # extract details from the POJO object
        username = message_pojo.email.split('@')[0]
        assignment_id = message_pojo.id
        submission_id = message_pojo.submissionID
        attempt = message_pojo.attempt
        submission_status = message_pojo.status
        submission_message = message_pojo.message
        file_name = username + '_' + str(assignment_id) + '_' + str(attempt) + '.zip'

        success = download_from_url(message_pojo.url, '/tmp/', file_name)
        if success:
            print("Zip file downloaded and extracted successfully.")
            upload_to_gcs('/tmp/' + file_name, file_name, assignment_id, username)
            print("File uploaded to GCS successfully.")
            send_email(message_pojo, message_pojo.email, file_name)
            print("Email sent successfully.")

            print("trying to insert Data into DynamoDB...")
            if submission_status == "SUCCESS":
                update_dynamodb(username, assignment_id, message_pojo.url, f'/{username}/{assignment_id}/{file_name}', message_pojo.timestamp)
                # insert_data_into_dynamodb(dynamodb_table, {'id': assignment_id, 'username': username, 'attempt': attempt, 'submission_id': submission_id, 'submission_status': submission_status, 'submission_message': submission_message})
                print("Data inserted into DynamoDB successfully!")
        else:
            send_email(message_pojo, message_pojo.email, file_name)
            print("Failed to download or extract the zip file.")

    except Exception as e:
        print(f"Error processing SNS message: {e}")

    return message  