from datetime import datetime
import boto3
from pprint import pprint
import json
client = boto3.client('dynamodb', 
    endpoint_url='http://localhost:8000'
)
table_name = "ample001"

payments = [
    {
        "payment_id":"payment_id1",
        "amount_received":5000
    }
]
service_types = [
    {
        "service_type_id":"service_type_id1",
        "name":"name1",
        "country":"VN",
        "currencies":"currencies1",
        "mode":"mode1",
    }
]
promo_codes = [
    {
        "promo_code_id":"promo_code_id1",
        "percent_dollar":20,
        "code":"code_abc",
        "amount":8,
        "start":'2022-08-17T03:03:59.952Z',
        "end":"2022-09-16T03:03:59.952Z",
    }
]

terminals = [
    {
        "terminal_id":"terminal_id1",
        "terminal_number":"terminal_number1",
        "terminal_code":"terminal_code1",
        "branch_id":"branch_id1",
        "teller_id":"teller_id1",
    }
]
branches = [
    {
        "branch_id":"branch_id1",
        "branch_name":"branch_name1",
        "branch_code":"branch_code1",
        "branch_country":"VN",
        "branch_phone":"branch_phone1",
        "branch_opening_hour":"branch_opening_hour1",
        "branch_location":"branch_location1",
        "stock_id":"stock_id1"
    }   
]
tellers = [
    {
        "teller_id":"teller_id1",
        "teller_username":"teller_username1"
    }
]
stocks = [
    {
        "stock_id":"stock_id1",
        "USD":100,
        "SGD":200,
    }
]



customers = [
    # sender
    # customer_id => backend gen, need to check duplicate
    {
        "customer_id":"customer_id1",
        "customer_information":{
            "first_name": "First",
            "last_name": "last name",
            "second_language_name": "33443",
            "personal_id": "33333",
            "personal_id_type": "Passport",
            "personal_id_expiry": "1728000",
            "date_of_birth": "816048000",
            "country_of_birth": "Algeria",
            "nationality": "Algeria",
            "kyc_document": []
        },
        "company_information":None,
        "contact_name":"First",
        "contact_phone_number":"+84982149607",
        "contact_email":"thithanh@gmail.com",
        "contact_address":"245 saigon",
        "compliance_country":"Local",
        "compliance_industry":"",
        "source_of_fund":"Debt capital",
        "occupation":"Chief Executives",
        "customer_type":"Individual",
        "customer_location":"Local",
        "remark":"",
        "created_at":"2022-08-17T03:03:59.952Z",
    },
    # receiver
    # customer_id => backend gen, need to check duplicate
    {
        "customer_id":"customer_id2",
        "customer_information":{
            "first_name": "First",
            "last_name": "last name 2",
            "second_language_name": "33443",
            "personal_id": "33333",
            "personal_id_type": "Passport",
            "personal_id_expiry": "1728000",
            "date_of_birth": "816048000",
            "country_of_birth": "Algeria",
            "nationality": "Algeria",
            "kyc_document": []
        },
        "company_information":None,
        "contact_name":"First",
        "contact_phone_number":"+84982149607",
        "contact_email":"thithanh@gmail.com",
        "contact_address":"245 saigon",
        "compliance_country":"Local",
        "compliance_industry":"",
        "source_of_fund":"Debt capital",
        "occupation":"Chief Executives",
        "customer_type":"Individual",
        "customer_location":"Local",
        "remark":"",
        "created_at":"2022-08-17T03:03:59.952Z",
    }
] 
banks = [
    {
        "bank_id": "bank_id1",
        "bank_name": "bank name 1",
        "bank_2nd_language": "bank in 2nd language",
        "bank_code": "bank code",
        "branch_name": "branch name 1",
        "branch_2nd_language": "branch 2",
        "branch_code": "branch code",
        "account_no": "1555555",
        "remark": ""
    }
]
transactions = [
    {
        "transaction_id":"transaction_id1",
        "transaction_datetime":"2022-08-17T03:03:59.952Z",
        "source":"source",
        "teller":{
            "teller_id":"teller_id1",
            "teller_username":"teller_username1"
        },
        "sender":customers[0],
        "receiver":customers[1],
        "transaction_mode":"Western Union",
        "service_type":service_types[0],
        "service_type_system":"Send",
        "receipt":{
            "receipt_number":"20220817",
            "receipt_date":"2022-08-17T03:03:59.952Z"
        },
        "rate": {
            "country": "Albania",
            "currency": "EUR",
            "rate": 0.8,
            "inverse_rate": 1.25
        },

        "source_currency": "EUR",
        "source_amount": "1111",
        "destination_currency": "SGD",
        "destination_amount": "1388.75",

        "promo_code":promo_codes[0],
        "status":"pending",
        "tendering": {
            "payment_mode": {
                "cash": "11",
                "nets": "2",
                "dbs": "2",
                "maybank": "2",
                "banktransfer": "2",
                "total": "19"
            },
            "source_amount": "1111",
            "fees": 10,
            "promo": {
                "percent_dollar": 0,
                "amount": 3
            },
            "status": "paid"
        },
        "bank_detail":banks[0],
        "terminal":terminals[0],
        "branch":branches[0],
        "payment":payments[0],
        "created_at":"2022-08-17T03:03:59.952Z"
    }
]

class Payment():
    def __init__(self,**properties):
        self.SK = f"payment_id{properties.get('payment_id')}"
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        Item={
                **self.properties,
                "type": {"S": 'Payment' },
                
                "PK": {"S": 'Payment' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": 'METADATA' },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": 'METADATA' },

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": 'METADATA' },

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": 'METADATA' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": 'METADATA' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": 'METADATA' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )


class ServiceType():
    def __init__(self,**properties):
        self.SK = f"service_type_id{properties.get('service_type_id')}"
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        service_type = self.properties
        Item={
                **self.properties,
                "type": {"S": 'ServiceType' },
                
                "PK": {"S": 'ServiceType' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": f'mode{service_type["mode"]}#country{service_type["country"]}#currency{service_type["currency"]}#name{service_type["name"]}' },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": 'METADATA' },

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": 'METADATA' },

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": 'METADATA' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": 'METADATA' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": 'METADATA' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )


class PromoCode():
    def __init__(self,**properties):
        self.SK = f"promo_code_id{properties.get('promo_code_id')}"
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        service_type = self.properties
        Item={
                **self.properties,
                "type": {"S": 'PromoCode' },
                
                "PK": {"S": 'PromoCode' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": 'METADATA' },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": 'METADATA' },

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": 'METADATA' },

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": 'METADATA' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": 'METADATA' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": 'METADATA' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )



class Terminal():
    def __init__(self,**properties):
        self.SK = f"branch_id{properties.get('branch_id')}#terminal_id{properties.get('terminal_id')}"
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        Item={
                **self.properties,
                "type": {"S": 'Terminal' },
                
                "PK": {"S": 'Terminal' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": f'teller_id{self.properties.get("teller_id")}' },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": 'METADATA' },

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": 'METADATA' },

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": 'METADATA' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": 'METADATA' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": 'METADATA' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )




class Branch():
    def __init__(self,**properties):
        self.SK = f"branch_id{properties.get('branch_id')}"
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        Item={
                **self.properties,
                "type": {"S": 'Branch' },
                
                "PK": {"S": 'Branch' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": 'METADATA' },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": 'METADATA' },

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": 'METADATA' },

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": 'METADATA' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": 'METADATA' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": 'METADATA' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )



class Teller():
    def __init__(self,**properties):
        self.SK = f"teller_id{properties.get('teller_id')}"

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        Item={
                **self.properties,
                "type": {"S": 'Teller' },
                
                "PK": {"S": 'Teller' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": 'METADATA' },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": 'METADATA' },

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": 'METADATA' },

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": 'METADATA' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": 'METADATA' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": 'METADATA' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )


class Stock():
    def __init__(self,**properties):
        self.SK = f"stock_id{properties.get('stock_id')}"

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        Item={
                **self.properties,
                "type": {"S": 'Stock' },
                
                "PK": {"S": 'Stock' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": 'METADATA' },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": 'METADATA' },

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": 'METADATA' },

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": 'METADATA' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": 'METADATA' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": 'METADATA' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )


class Customer():
    def __init__(self,**properties):
        self.SK = f"customer_id{properties.get('customer_id')}"

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        Item={
                **self.properties,
                "type": {"S": 'Customer' },
                
                "PK": {"S": 'Customer' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": f'{self.properties.get("customer_information",{}).get("personal_id")}'},

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": f'{self.properties.get("contact_phone_number")}'},

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": f'{self.properties.get("company_information",{}).get("uen")}'},

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": f'{self.properties.get("contact_name")}'},

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": f'{self.properties.get("contact_email")}'},
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": f'customer_type{self.properties.get("customer_type")}#created_at{self.properties.get("created_at")}' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )



class Customer():
    def __init__(self,**properties):
        self.SK = f"customer_id{properties.get('customer_id')}"

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        customer = self.properties
        GSI6_SK = f"customer_type{customer['customer_type']}created_at{customer['created_at']}"
    
        Item={
                **self.properties,
                "type": {"S": 'Customer' },
                
                "PK": {"S": 'Customer' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": f'{self.properties.get("customer_information",{}).get("personal_id")}'},

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": f'{self.properties.get("contact_phone_number")}}',

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": f'{self.properties.get("company_information",{}).get("uen")}'},

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": f'{self.properties.get("contact_name")}'},

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": f'{self.properties.get("contact_email")}'},
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {
                    "S": GSI6_SK
                }
        }
        print(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )


class Bank():
    def __init__(self,**properties):
        self.SK = f"bank_id{properties.get('bank_id')}"

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        Item={
                **self.properties,
                "type": {"S": 'Bank' },
                
                "PK": {"S": 'Bank' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": 'METADATA' },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": 'METADATA' },

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": 'METADATA' },

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": 'METADATA' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": 'METADATA' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": 'METADATA' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )


class Transaction():
    def __init__(self,**properties):
        self.SK = f"branch_id{properties.get('branch',{}).get('branch_id',{})}#terminal_id{properties.get('terminal',{}).get('terminal_id')}#tran_mode{properties.get('transaction_mode')}#created_at{created_at}"

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                value_data_type = 'M'
            elif isinstance(value,str):
                value_data_type = 'S'
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
            else:
                value_data_type = 'S'
            self.properties[key] = {
                value_data_type:'%s'%value
            } 
        

    def insert(self):
        transaction = self.properties
        Item={
                **self.properties,
                "type": {"S": 'Transaction' },
                
                "PK": {"S": 'Transaction' },
                "SK": {"S":  self.SK},
                
                "GSI_created_at_PK": {"S": 'METADATA' },
                "GSI_created_at_SK": {"S": self.properties.get('created_at') if self.properties.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA' },
                "GSI1_SK": {"S": customer_id },

                "GSI2_PK": {"S": 'METADATA' },
                "GSI2_SK": {"S": transaction.get('sender',{}).get('customer_id')},

                "GSI3_PK": {"S": 'METADATA' },
                "GSI3_SK": {"S": transaction.get('payment',{}).get('payment_id')},

                "GSI4_PK": {"S": 'METADATA' },
                "GSI4_SK": {"S": f'service_type_system{transaction["service_type_system"]}#branch_id{transaction["branch_id"]}#transaction_mode{transaction["transaction_mode"]}' },

                "GSI5_PK": {"S": 'METADATA' },
                "GSI5_SK": {"S": f'branch_id{transaction["branch_id"]}#terminal_id{transaction["terminal_id"]}#created_at{transaction["created_at"]}' },
                
                "GSI6_PK": {"S": 'METADATA' },
                "GSI6_SK": {"S": f'branch_id{transaction["branch_id"]}#created_at{transaction["created_at"]}' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )

if __name__ =="__main__":
    for item in payments:
        instance = Payment(**item)
        instance.insert()
    
    # for item in service_types:
    #     instance = ServiceType(**item)
    #     instance.insert()
    
    # for item in promo_codes:
    #     instance = PromoCode(**item)
    #     instance.insert()
    
    # for item in terminals:
    #     instance = Terminal(**item)
    #     instance.insert()

    # for item in branches:
    #     instance = Branch(**item)
    #     instance.insert()

    # for item in tellers:
    #     instance = Teller(**item)
    #     instance.insert()
    
    # for item in stocks:
    #     instance = Stock(**item)
    #     instance.insert()