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
const region = 'us-east-1';
const dynamoDbClient = createDynamoDbClient(region);

// Create the input for createTable call
const createTableInput = createCreateTableInput();

// Call DynamoDB's createTable API
executeCreateTable(dynamoDbClient, createTableInput).then(() => {
        console.info('CreateTable API call has been executed.')
    }
);

function createDynamoDbClient(regionName) {
    // Set the region
    AWS.config.update({region: regionName});
    // Use the following config instead when using DynamoDB Local
    // AWS.config.update({region: 'localhost', endpoint: 'http://localhost:8000', accessKeyId: 'access_key_id', secretAccessKey: 'secret_access_key'});
    return new AWS.DynamoDB();
}

function createCreateTableInput() {
    return {
        "TableName": "Employee",
        "KeySchema": [
            {
                "AttributeName": "LoginAlias",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "JoinDate",
                "KeyType": "RANGE"
            }
        ],
        "BillingMode": "PROVISIONED",
        "AttributeDefinitions": [
            {
                "AttributeName": "LoginAlias",
                "AttributeType": "S"
            },
            {
                "AttributeName": "JoinDate",
                "AttributeType": "S"
            },
            {
                "AttributeName": "LastName",
                "AttributeType": "S"
            },
            {
                "AttributeName": "FirstName",
                "AttributeType": "S"
            }
        ],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
        },
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "Name",
                "KeySchema": [
                    {
                        "AttributeName": "LastName",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "FirstName",
                        "KeyType": "RANGE"
                    }
                ],
                "Projection": {
                    "ProjectionType": "ALL"
                },
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1
                }
            }
        ]
    }
}

async function executeCreateTable(dynamoDbClient, createTableInput) {
    // Call DynamoDB's createTable API
    try {
        const createTableOutput = await dynamoDbClient.createTable(createTableInput).promise();
        console.info('Successfully created table.');
        // Handle createTableOutput
    } catch (err) {
        handleCreateTableError(err);
    }
}

// Handles errors during CreateTable execution. Use recommendations in error messages below to
// add error handling specific to your application use-case.
function handleCreateTableError(err) {
    if (!err) {
        console.error('Encountered error object was empty');
        return;
    }
    if (!err.code) {
        console.error(`An exception occurred, investigate and configure retry strategy. Error: ${JSON.stringify(err)}`);
        return;
    }
    switch (err.code) {
        case 'LimitExceededException':
            console.error(`Number of simultaneous table operations may exceed the limit. Up to 50 simultaneous table operations are allowed per account.` +
                `You can have up to 25 such requests running at a time; however, if the table or index specifications are complex,` +
                `DynamoDB might temporarily reduce the number of concurrent operations. Consider retry it later. Error: ${err.message}`);
            return;
        case 'ResourceInUseException':
            console.error(`Table is already existed. Change the table name before retrying. Error: ${err.message}`);
            return;
        default:
            break;
        // Common DynamoDB API errors are handled below
    }
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
