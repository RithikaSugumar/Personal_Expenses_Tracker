import json
import boto3
from datetime import datetime

# DynamoDB table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ExpensesTable')

# Kinesis client
kinesis = boto3.client('kinesis')

def lambda_handler(event, context):
    
    # Get JSON data from API Gateway
    body = json.loads(event['body'])
    
    name = body.get("name")
    expense = body.get("expense")
    payment_type = body.get("payment_type")
    timestamp = str(datetime.utcnow())
    
    # 1️⃣ Store in DynamoDB (structured data)
    item = {
        "user_name": name,
        "expense_amount": float(expense),
        "payment_type": payment_type,
        "timestamp": timestamp
    }
    
    table.put_item(Item=item)
    
    # 2️⃣ Send event to Kinesis (real-time streaming)
    kinesis.put_record(
        StreamName="ExpenseStream",
        Data=json.dumps(item),
        PartitionKey=name
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps("Expense stored and streamed successfully!")
    }
