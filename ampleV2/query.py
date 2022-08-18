from pprint import pprint
from typing import Any
import boto3
import insert

client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
table_name = "ample001"


'''
key_condition_expression = "GSI1_PK = :GSI1_PK and begins_with(GSI1_SK,:GSI1_SK)"
expression_values = {
    ":GSI1_PK": {"S": 'METADATA_BRANCH'},
    ":GSI1_SK": {"S": f'mode{mode}#country{country}#currency{currency}'},
}

resp = client.query(
    TableName=table_name,
    IndexName='GSI1',
    KeyConditionExpression=key_condition_expression,
    ExpressionAttributeValues=expression_values
)
Items = resp.get('Items')
'''



# stock_id = 'stock_id1'
# stocks = insert.Stock.get_stocks_by_stock_id(stock_id)
# pprint(stocks)


# branches = insert.Branch.get_all_branch()
# pprint(branches)

# branch_id = 'branch_id1'
# terminals = insert.Terminal.get_terminals_by_branch_id(branch_id)
# pprint(terminals)

# teller_id = 'teller_id1'
# terminals = insert.Terminal.get_terminals_by_teller_id(teller_id)
# pprint(terminals)

# payments = insert.Payment.get_all_payment()
# pprint(payments)


payment_id = 'payment_id1'
transactions = insert.Transaction.get_all_transactions_by_payment_id(payment_id)
pprint(transactions)
