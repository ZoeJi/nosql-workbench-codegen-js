// ------------ NodeJS runtime ---------------
// Add aws-sdk in package.json as a dependency
// Example:
// {
//     "dependencies": {
//         "aws-sdk": "^2.0.9",
//     }
// }
// Create your credentials file at ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users)
// Format of the above file should be:
//  [default]
//  aws_access_key_id = YOUR_ACCESS_KEY_ID
//  aws_secret_access_key = YOUR_SECRET_ACCESS_KEY

const AWS = require('aws-sdk');

// Create the DynamoDB Client with the region you want
const region = 'eu-west-1';
const dynamoDbClient = createDynamoDbClient(region);

// Create the input for batchExecuteStatement call
const batchExecuteStatementInput = createBatchExecuteStatementInput();

// Call DynamoDB's batchExecuteStatement API
executeBatchExecuteStatement(dynamoDbClient, batchExecuteStatementInput).then(() => {
        console.info('BatchExecuteStatement API call has been executed.')
    }
);

function createDynamoDbClient(regionName) {
    // Set the region
    AWS.config.update({region: regionName});
    AWS.config.update({endpoint: 'http://127.0.0.1:8080'});
    // Use the following config instead when using DynamoDB Local
    // AWS.config.update({region: 'localhost', endpoint: 'http://localhost:8000', accessKeyId: 'access_key_id', secretAccessKey: 'secret_access_key'});
    return new AWS.DynamoDB();
}

function createBatchExecuteStatementInput() {
    return {
        "Statements": [
            {
                "Statement": "select d1 from test_table where pk = ? and hk = ?",
                "Parameters": [
                    {
                        "S": "p1"
                    },
                    {
                        "S": "h5"
                    }
                ]
            },
            {
                "Statement": "select data from test_table where pk = ? and hk = ?",
                "Parameters": [
                    {
                        "S": "p1"
                    },
                    {
                        "S": "h10"
                    }
                ]
            }
        ]
    }
}

async function executeBatchExecuteStatement(dynamoDbClient, batchExecuteStatementInput) {
    // Call DynamoDB's batchExecuteStatement API
    try {
        const batchExecuteStatementOutput = await dynamoDbClient.batchExecuteStatement(batchExecuteStatementInput).promise();
        console.info('BatchExecuteStatement executed successfully.');
        // Handle batchExecuteStatementOutput
        console.log(JSON.stringify(batchExecuteStatementOutput))
    } catch (err) {
        handleBatchExecuteStatementError(err);
    }
}

// Handles errors during BatchExecuteStatement execution. Use recommendations in error messages below to
// add error handling specific to your application use-case.
function handleBatchExecuteStatementError(err) {
    if (!err) {
        console.error('Encountered error object was empty');
        return;
    }
    if (!err.code) {
        console.error(`An exception occurred, investigate and configure retry strategy. Error: ${JSON.stringify(err)}`);
        return;
    }
    // here are no API specific errors to handle for BatchExecuteStatement, common DynamoDB API errors are handled below
    handleCommonErrors(err);
}

function handleCommonErrors(err) {
    switch (err.code) {
        case 'InternalServerError':
            console.error(`Internal Server Error, generally safe to retry with exponential back-off. Error: ${err.message}`);
            return;
        case 'ProvisionedThroughputExceededException':
            console.error(`Request rate is too high. If you're using a custom retry strategy make sure to retry with exponential back-off. `
                + `Otherwise consider reducing frequency of requests or increasing provisioned capacity for your table or secondary index. Error: ${err.message}`);
            return;
        case 'ResourceNotFoundException':
            console.error(`One of the tables was not found, verify table exists before retrying. Error: ${err.message}`);
            return;
        case 'ServiceUnavailable':
            console.error(`Had trouble reaching DynamoDB. generally safe to retry with exponential back-off. Error: ${err.message}`);
            return;
        case 'ThrottlingException':
            console.error(`Request denied due to throttling, generally safe to retry with exponential back-off. Error: ${err.message}`);
            return;
        case 'UnrecognizedClientException':
            console.error(`The request signature is incorrect most likely due to an invalid AWS access key ID or secret key, fix before retrying. `
                + `Error: ${err.message}`);
            return;
        case 'ValidationException':
            console.error(`The input fails to satisfy the constraints specified by DynamoDB, `
                + `fix input before retrying. Error: ${err.message}`);
            return;
        case 'RequestLimitExceeded':
            console.error(`Throughput exceeds the current throughput limit for your account, `
                + `increase account level throughput before retrying. Error: ${err.message}`);
            return;
        default:
            console.error(`An exception occurred, investigate and configure retry strategy. Error: ${err.message}`);
            return;
    }
}
