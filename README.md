# AWS Lambda Function - servlerless

This Lambda function is designed to handle events triggered by an Amazon Simple Notification Service (SNS) topic. The primary purpose of this function is to process messages received from the SNS topic, which typically contain information about a submission in an educational context.

## Functionality Overview

1.  **Environment Variables Setup:**
    
    -   The function retrieves essential configuration parameters from environment variables. These include Google Cloud Storage credentials, S3 bucket name, SMTP (Simple Mail Transfer Protocol) server details for sending emails, and the name of a DynamoDB table.
2.  **SNS Message Processing:**
    
    -   Upon receiving an event from the SNS topic, the Lambda function extracts the SNS message payload. This payload is expected to be a JSON string representing an object of the `SNSMessage` class, containing details about a submission.
3.  **File Handling:**
    
    -   The function downloads a ZIP file from a provided URL, extracts its contents, and saves it to the `/tmp/` directory. The extracted files are then uploaded to Google Cloud Storage (GCS) under a specific folder structure.
4.  **Email Notification:**
    
    -   Depending on the success or failure of the submission, an email is composed and sent to the submitter. The email includes details such as submission status, relevant messages, and file paths.
5.  **DynamoDB Integration:**
    
    -   If the submission is successful, the Lambda function updates a DynamoDB table with information about the submission. This includes details such as user email, assignment ID, submission URL, file path, and timestamp.
6.  **Error Handling:**
    
    -   The function includes error handling to catch and log any exceptions that might occur during the processing of SNS messages or other operations.

## Environment Variables

-   `GOOGLE_CREDENTIALS`: Base64-encoded Google Cloud Service Account JSON key file.
-   `BUCKET_NAME`: The name of the Google Cloud Storage bucket.
-   `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SENDER_EMAIL`: SMTP server details for sending emails.
-   `DYNAMODB_TABLE`: The name of the DynamoDB table for storing submission information.

## Usage

1.  **Google Cloud Storage Setup:**
    
    -   Ensure that the Google Cloud Storage bucket specified in the `BUCKET_NAME` environment variable exists and is accessible by the provided service account.
2.  **SMTP Server Configuration:**
    
    -   Configure the SMTP server details to enable email notifications. Ensure that the specified SMTP server is accessible from the Lambda function.
3.  **DynamoDB Setup:**
    
    -   Create a DynamoDB table with the name specified in the `DYNAMODB_TABLE` environment variable. Define the necessary attributes to store submission information.
4.  **Lambda Trigger Setup:**
    
    -   Configure an SNS topic to trigger this Lambda function. Ensure that the SNS message payload adheres to the expected format for the `SNSMessage` class.
5.  **Environment Variable Configuration:**
    
    -   Set the required environment variables in the Lambda function configuration.

## Example SNS Message Payload

jsonCopy code

`{
  "id": "123456",
  "submissionID": "789012",
  "status": "SUCCESS",
  "message": "Submission processed successfully.",
  "url": "https://example.com/submission.zip",
  "email": "user@example.com",
  "attempt": 1,
  "timestamp": "2023-11-29T12:34:56Z"
}` 

## Notes

-   Ensure that the Lambda function has the necessary permissions to access Google Cloud Storage, send emails via SMTP, and interact with DynamoDB.
-   The function assumes that the SNS message payload adheres to the structure of the `SNSMessage` class.
-   Review the code and adjust as needed based on specific requirements and configurations.