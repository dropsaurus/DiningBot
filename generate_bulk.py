import boto3
import json

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

response = table.scan()
items = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    items.extend(response['Items'])

with open('bulk_data.txt', 'w') as f:
    for item in items:
        business_id = item.get('BusinessID')
        cuisine = item.get('Cuisine')
        
        if business_id and cuisine:
            f.write(json.dumps({"index": {"_index": "restaurants"}}) + '\n')
            doc = {"type": "Restaurant", "RestaurantID": business_id, "Cuisine": cuisine.lower()}
            f.write(json.dumps(doc) + '\n')

print(f"Generated {len(items)} restaurants")
