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




# customers = insert.Customer.get_all_customers()
# pprint(customers)


# personal_id = '33333'
# customers = insert.Customer.get_customers_by_personal_id(personal_id)
# pprint(customers)

# contact_phone_number = '+849821'
# customers = insert.Customer.get_customers_by_contact_phone_number(contact_phone_number)
# pprint(customers)

# uen = '+849821'
# customers = insert.Customer.get_customers_by_uen(uen)
# pprint(customers)

# contact_name = 'First'
# customers = insert.Customer.get_customers_by_contact_name(contact_name)
# pprint(customers)

# contact_email = 'thithanh@gmail.'
# customers = insert.Customer.get_customers_by_contact_email(contact_email)
# pprint(customers)



# payment_id = 'payment_id1'
# transactions = insert.Transaction.get_all_transactions_by_payment_id(payment_id)
# pprint(transactions)

# transactions = insert.Transaction.get_all_transactions()
# pprint(transactions)

# customer_id = 'customer_id1'
# transactions = insert.Transaction.get_all_transactions_by_sender_id(customer_id)
# pprint(transactions)


# customer_id = 'customer_id2'
# transactions = insert.Transaction.get_all_transactions_by_receiver_id(customer_id)
# pprint(transactions)


# branch_id = 'branch_id1'
# from_date = '2022-08-17T03:03:59.952Z'
# to_date = '2022-08-17T03:03:59.952Z'
# transactions = insert.Transaction.get_trans_by_branch(branch_id,from_date,to_date)
# pprint(transactions)

# branch_id = 'branch_id1'
# terminal_id = 'terminal_id1'
# from_date = '2022-08-17T03:03:59.952Z'
# to_date = '2022-08-17T03:03:59.952Z'
# transactions = insert.Transaction.get_trans_by_branch_and_terminal(branch_id,terminal_id,from_date,to_date)
# pprint(transactions)


branch_id = 'branch_id1'
terminal_id = 'terminal_id1'
tran_mode = 'Western Union'
from_date = '2022-08-17T03:03:59.952Z'
to_date = '2022-08-17T03:03:59.952Z'
transactions = insert.Transaction.get_trans_by_branch_and_terminal_and_mode(branch_id,terminal_id,tran_mode,from_date,to_date)
pprint(transactions)
