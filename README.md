# SQLTools DynamoDB Driver

DynamoDB Driver for the vscode-sqltools extension.

This extension only supports [PartiQL syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html).

## IAM security policies with PartiQL for DynamoDB

The following IAM policy grants permissions to working with this extension.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:ListTables"],
      "Resource": ["<your_dynamo_resources>"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:Scan",
        "dynamodb:PartiQLInsert",
        "dynamodb:PartiQLUpdate",
        "dynamodb:PartiQLDelete",
        "dynamodb:PartiQLSelect"
      ],
      "Resource": ["<your_dynamo_resources>"]
    }
  ]
}
```

For example:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:ListTables"],
      "Resource": ["*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:Scan",
        "dynamodb:PartiQLInsert",
        "dynamodb:PartiQLUpdate",
        "dynamodb:PartiQLDelete",
        "dynamodb:PartiQLSelect"
      ],
      "Resource": ["arn:aws:dynamodb:us-east-1:12345678910:table/*"]
    }
  ]
}
```
