from datetime import datetime
import boto3
from pprint import pprint
import json
client = boto3.client('dynamodb', 
    endpoint_url='http://localhost:8000'
)
table_name = "ample001"

def filterAttrItemRes(func):
    def wrapperFunc(*args,**kargs):
        Items = func(*args,**kargs)
        L_items = []
        for item in Items:
            d= {}
            for key,value in item.items():
                if not key.startswith('PK') and not key.startswith('SK') and not key.startswith('GSI') and not key=='type':
                    d[key] = value[list(value.keys())[0]]
            L_items.append(d)
        return L_items
    return wrapperFunc
    

class Payment():
    def __init__(self,**properties):
        self.payment = properties
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass
                 
        

    def insert(self):
        payment = self.payment
        Item={
                **self.properties,
                "type": {"S": 'Payment' },
                
                "PK": {"S": 'Payment' },
                "SK": {"S":  f"payment_id{payment['payment_id']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_PAYMENT' },
                "GSI_created_at_SK": {"S": payment.get('created_at') if payment.get('created_at') else datetime.now().isoformat()},

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
    
    @staticmethod
    @filterAttrItemRes
    def get_all_payment():
        key_condition_expression = "PK = :PK"
        expression_values = {
            ":PK": {"S": 'Payment'},
        }

        resp = client.query(
            TableName=table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items 



class ServiceType():
    def __init__(self,**properties):
        self.service_type = properties
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass
            
        

    def insert(self):
        service_type = self.service_type
        Item={
                **self.properties,
                "type": {"S": 'ServiceType' },
                
                "PK": {"S": 'ServiceType' },
                "SK": {"S":  f"service_type_id{service_type['service_type_id']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_SERVICETYPE' },
                "GSI_created_at_SK": {"S": service_type.get('created_at') if service_type.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA_SERVICETYPE' },
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

   
    @staticmethod
    def get_all_mode():
        key_condition_expression = "GSI1_PK = :GSI1_PK"
        expression_values = {
            ":GSI1_PK": {"S": 'METADATA_SERVICETYPE'},
        }

        resp = client.query(
            TableName=table_name,
            IndexName='GSI1',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        list_modes = []
        for item in Items:
            mode = item['GSI1_SK']['S'].split('#')[0]
            mode = mode[len('mode'):]
            list_modes.append(mode)
        list_modes = list(dict.fromkeys(list_modes))

        return list_modes

    @staticmethod
    def search_service_type(mode,country,currency):
        key_condition_expression = "GSI1_PK = :GSI1_PK and begins_with(GSI1_SK,:GSI1_SK)"
        expression_values = {
            ":GSI1_PK": {"S": 'METADATA_SERVICETYPE'},
            ":GSI1_SK": {"S": f'mode{mode}#country{country}#currency{currency}'},
        }

        resp = client.query(
            TableName=table_name,
            IndexName='GSI1',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        list_service_types = []
        for item in Items:
            name = item['GSI1_SK']['S'].split('#')[-1]
            name = name[len('name'):]
            service_type_id = item['SK']['S']
            service_type_id = service_type_id[len('service_type_id'):]
            list_service_types.append({
                "service_type_id":service_type_id,
                "mode":mode,
                "country":country,
                "currency":currency,
                "name":name
            })
        return list_service_types



class PromoCode():
    def __init__(self,**properties):
        self.promo_code = properties
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass
             
        

    def insert(self):
        promo_code = self.promo_code
        Item={
                **self.properties,
                "type": {"S": 'PromoCode' },
                
                "PK": {"S": 'PromoCode' },
                "SK": {"S":  f"promo_code_id{promo_code['promo_code_id']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_PROMOCODE' },
                "GSI_created_at_SK": {"S": promo_code.get('created_at') if promo_code.get('created_at') else datetime.now().isoformat()},

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
        self.terminal = properties
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass 
        

    def insert(self):
        terminal = self.terminal
        Item={
                **self.properties,
                "type": {"S": 'Terminal' },
                
                "PK": {"S": 'Terminal' },
                "SK": {"S":  f"branch_id{terminal['branch_id']}#terminal_id{terminal['terminal_id']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_TERMINAL' },
                "GSI_created_at_SK": {"S": terminal.get('created_at') if terminal.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA_TERMINAL' },
                "GSI1_SK": {"S": f'teller_id{terminal["teller_id"]}' },

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

    @staticmethod
    @filterAttrItemRes
    def get_terminals_by_branch_id(branch_id):
        key_condition_expression = "PK = :PK and begins_with(SK,:SK)"
        expression_values = {
            ":PK": {"S": 'Terminal'},
            ":SK": {"S": f"branch_id{branch_id}"},
        }

        resp = client.query(
            TableName=table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_terminals_by_teller_id(teller_id):
        key_condition_expression = "GSI1_PK = :GSI1_PK and begins_with(GSI1_SK,:GSI1_SK)"
        expression_values = {
            ":GSI1_PK": {"S": 'METADATA_TERMINAL'},
            ":GSI1_SK": {"S": f'teller_id{teller_id}'},
        }

        resp = client.query(
            TableName=table_name,
            IndexName='GSI1',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items


class Branch():
    def __init__(self,**properties):
        self.branch = properties
        
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass 
        
    def insert(self):
        branch = self.branch
        Item={
                **self.properties,
                "type": {"S": 'Branch' },
                
                "PK": {"S": 'Branch' },
                "SK": {"S":  f"branch_id{branch['branch_id']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_BRANCH' },
                "GSI_created_at_SK": {"S": branch.get('created_at') if branch.get('created_at') else datetime.now().isoformat()},

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

    @staticmethod
    @filterAttrItemRes
    def get_all_branch():
        key_condition_expression = "PK = :PK"
        expression_values = {
            ":PK": {"S": 'Branch'},
        }

        resp = client.query(
            TableName=table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items


class Teller():
    def __init__(self,**properties):
        self.teller = properties
        

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass
        

    def insert(self):
        teller = self.teller
        Item={
                **self.properties,
                "type": {"S": 'Teller' },
                
                "PK": {"S": 'Teller' },
                "SK": {"S":  f"teller_id{teller['teller_id']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_TELLER' },
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
        self.stock = properties
        

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass

    def insert(self):
        stock = self.stock
        Item={
                **self.properties,
                "type": {"S": 'Stock' },
                
                "PK": {"S": 'Stock' },
                "SK": {"S":  f"stock_id{stock['stock_id']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_STOCK' },
                "GSI_created_at_SK": {"S": stock.get('created_at') if stock.get('created_at') else datetime.now().isoformat()},

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

    @staticmethod
    @filterAttrItemRes
    def get_stocks_by_stock_id(stock_id):
        key_condition_expression = "PK = :PK and begins_with(SK,:SK)"
        expression_values = {
            ":PK": {"S": 'Stock'},
            ":SK": {"S": f'stock_id{stock_id}'},
        }

        resp = client.query(
            TableName=table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items


class Customer():
    def __init__(self,**properties):
        self.customer = properties
        
        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                if key == 'customer_information':
                    self.properties[key] = {
                        'M':{
                            "first_name": {"S":value['first_name']},
                            "last_name": {"S":value['last_name']},
                            "second_language_name": {"S":value['second_language_name']},
                            "personal_id": {"S":value['personal_id']},
                            "personal_id_type": {"S":value['personal_id_type']},
                            "personal_id_expiry": {"S":value['personal_id_expiry']},
                            "date_of_birth": {"S":value['date_of_birth']},
                            "country_of_birth": {"S":value['country_of_birth']},
                            "nationality": {"S":value['nationality']},
                            "kyc_document": {"L":[
                                {"S":e}
                                for e in value['kyc_document']
                            ]},
                        }
                    }
            elif isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass 
        

    def insert(self):
        customer = self.customer
        Item={
                **self.properties,
                "type": {"S": 'Customer' },
                
                "PK": {"S": 'Customer' },
                "SK": {"S":  f"customer_id{customer.get('customer_id')}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_CUSTOMER' },
                "GSI_created_at_SK": {"S": customer.get('created_at') if customer.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA_CUSTOMER' },
                "GSI1_SK": {"S": f'{customer.get("customer_information",{}).get("personal_id") if customer.get("customer_information",{}) else None}'},

                "GSI2_PK": {"S": 'METADATA_CUSTOMER' },
                "GSI2_SK": {"S": f'{customer.get("contact_phone_number")}'},

                "GSI3_PK": {"S": 'METADATA_CUSTOMER' },
                "GSI3_SK": {"S": f'{customer.get("company_information",{}).get("uen") if customer.get("company_information",{}) else None}'},

                "GSI4_PK": {"S": 'METADATA_CUSTOMER' },
                "GSI4_SK": {"S": f'{customer.get("contact_name")}'},

                "GSI5_PK": {"S": 'METADATA_CUSTOMER' },
                "GSI5_SK": {"S": f'{customer.get("contact_email")}'},
                
                "GSI6_PK": {"S": 'METADATA_CUSTOMER' },
                "GSI6_SK": {"S": f'customer_type{customer.get("customer_type")}#created_at{customer.get("created_at")}' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )

    @staticmethod
    @filterAttrItemRes
    def get_all_customers():
        key_condition_expression = "PK = :PK"
        expression_values = {
            ":PK": {"S": 'Customer'},
            }

        resp = client.query(
            TableName=table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_customers_by_personal_id(personal_id):
        key_condition_expression = "GSI1_PK = :GSI1_PK and begins_with(GSI1_SK,:GSI1_SK)"
        expression_values = {
            ":GSI1_PK": {"S": 'METADATA_CUSTOMER'},
            ":GSI1_SK": {"S": personal_id},
        }

        resp = client.query(
            TableName=table_name,
            IndexName='GSI1',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_customers_by_contact_phone_number(contact_phone_number):
        key_condition_expression = "GSI2_PK = :GSI2_PK and begins_with(GSI2_SK,:GSI2_SK)"
        expression_values = {
            ":GSI2_PK": {"S": 'METADATA_CUSTOMER'},
            ":GSI2_SK": {"S": contact_phone_number},
        }

        resp = client.query(
            TableName=table_name,
            IndexName='GSI2',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_customers_by_uen(uen):
        key_condition_expression = "GSI3_PK = :GSI3_PK and begins_with(GSI3_SK,:GSI3_SK)"
        expression_values = {
            ":GSI3_PK": {"S": 'METADATA_CUSTOMER'},
            ":GSI3_SK": {"S": uen},
        }

        resp = client.query(
            TableName=table_name,
            IndexName='GSI3',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_customers_by_contact_name(contact_name):
        key_condition_expression = "GSI4_PK = :GSI4_PK and begins_with(GSI4_SK,:GSI4_SK)"
        expression_values = {
            ":GSI4_PK": {"S": 'METADATA_CUSTOMER'},
            ":GSI4_SK": {"S": contact_name},
        }

        resp = client.query(
            TableName=table_name,
            IndexName='GSI4',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_customers_by_contact_email(contact_email):
        key_condition_expression = "GSI5_PK = :GSI5_PK and begins_with(GSI5_SK,:GSI5_SK)"
        expression_values = {
            ":GSI5_PK": {"S": 'METADATA_CUSTOMER'},
            ":GSI5_SK": {"S": contact_email},
        }

        resp = client.query(
            TableName=table_name,
            IndexName='GSI5',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items



class Bank():
    def __init__(self,**properties):
        self.bank = properties
        

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass
                 
        

    def insert(self):
        bank = self.bank
        Item={
                **self.properties,
                "type": {"S": 'Bank' },
                
                "PK": {"S": 'Bank' },
                "SK": {"S":  f"bank_id{bank['bank_id']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_BANK' },
                "GSI_created_at_SK": {"S": bank.get('created_at') if bank.get('created_at') else datetime.now().isoformat()},

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
        self.transaction = properties
        
        

        self.properties = {}
        for key, value in properties.items():
            if isinstance(value,dict):
                if key == 'receipt':
                    self.properties[key] = {
                        'M':{
                            "receipt_number": {"S":value['receipt_number']},
                            "receipt_date": {"S":value['receipt_date']}
                        }
                    }
                if key == 'rate':
                    self.properties[key] = {
                        'M':{
                            "country": {"S":value['country']},
                            "currency": {"S":value['currency']},
                            "rate": {"N":'%s'%value['rate']},
                            "inverse_rate": {"N":'%s'%value['inverse_rate']}
                        }
                    }
            elif isinstance(value,str):
                value_data_type = 'S'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            elif isinstance(value,int) or isinstance(value,float):
                value_data_type = 'N'
                self.properties[key] = {
                    value_data_type:'%s'%value
                }
            else:
                pass
             
        

    def insert(self):
        transaction = self.transaction
        Item={
                **self.properties,
                "type": {"S": 'Transaction' },
                
                "PK": {"S": 'Transaction' },
                "SK": {"S":  f"branch_id{transaction['branch_id']}#terminal_id{transaction['terminal_id']}#tran_mode{transaction['transaction_mode']}#created_at{transaction['created_at']}"},
                
                "GSI_created_at_PK": {"S": 'METADATA_TRANSACTION' },
                "GSI_created_at_SK": {"S": transaction.get('created_at') if transaction.get('created_at') else datetime.now().isoformat()},

                "GSI1_PK": {"S": 'METADATA_TRANSACTION' },
                "GSI1_SK": {"S": transaction.get('sender') },

                "GSI2_PK": {"S": 'METADATA_TRANSACTION' },
                "GSI2_SK": {"S": transaction.get('receiver')},

                "GSI3_PK": {"S": 'METADATA_TRANSACTION' },
                "GSI3_SK": {"S": transaction.get('payment_id')},

                "GSI4_PK": {"S": 'METADATA_TRANSACTION' },
                "GSI4_SK": {"S": f'service_type_system{transaction["service_type_system"]}#branch_id{transaction["branch_id"]}#transaction_mode{transaction["transaction_mode"]}' },

                "GSI5_PK": {"S": 'METADATA_TRANSACTION' },
                "GSI5_SK": {"S": f'branch_id{transaction["branch_id"]}#terminal_id{transaction["terminal_id"]}#created_at{transaction["created_at"]}' },
                
                "GSI6_PK": {"S": 'METADATA_TRANSACTION' },
                "GSI6_SK": {"S": f'branch_id{transaction["branch_id"]}#created_at{transaction["created_at"]}' },
        }
        # pprint(Item)
        client.put_item(
            TableName=table_name,
            Item=Item
        )
    
    @staticmethod
    @filterAttrItemRes
    def get_all_transactions_by_payment_id(payment_id):
        key_condition_expression = "GSI3_PK = :GSI3_PK and begins_with(GSI3_SK,:GSI3_SK)"
        expression_values = {
            ":GSI3_PK": {"S": 'METADATA_TRANSACTION'},
            ":GSI3_SK": {"S": payment_id},
        }
        resp = client.query(
            TableName=table_name,
            IndexName='GSI3',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_all_transactions_by_sender_id(customer_id):
        key_condition_expression = "GSI1_PK = :GSI1_PK and begins_with(GSI1_SK,:GSI1_SK)"
        expression_values = {
            ":GSI1_PK": {"S": 'METADATA_TRANSACTION'},
            ":GSI1_SK": {"S": customer_id},
        }
        resp = client.query(
            TableName=table_name,
            IndexName='GSI1',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_all_transactions_by_receiver_id(customer_id):
        key_condition_expression = "GSI2_PK = :GSI2_PK and begins_with(GSI2_SK,:GSI2_SK)"
        expression_values = {
            ":GSI2_PK": {"S": 'METADATA_TRANSACTION'},
            ":GSI2_SK": {"S": customer_id},
        }
        resp = client.query(
            TableName=table_name,
            IndexName='GSI2',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_all_transactions():
        key_condition_expression = "PK = :PK"
        expression_values = {
            ":PK": {"S": 'Transaction'}
        }
        resp = client.query(
            TableName=table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items


    @staticmethod
    @filterAttrItemRes
    def get_trans_by_branch(branch_id,from_date,to_date):
        key_condition_expression = "GSI6_PK = :GSI6_PK and GSI6_SK BETWEEN :GSI6_SK_FROM AND :GSI6_SK_TO"
        expression_values = {
            ":GSI6_PK": {"S": 'METADATA_TRANSACTION'},
            ":GSI6_SK_FROM": {"S": f"branch_id{branch_id}#created_at{from_date}"},
            ":GSI6_SK_TO": {"S": f"branch_id{branch_id}#created_at{to_date}"}
        }
        
        resp = client.query(
            TableName=table_name,
            IndexName='GSI6',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_trans_by_branch_and_terminal(branch_id,terminal_id,from_date,to_date):
        key_condition_expression = "GSI5_PK = :GSI5_PK and GSI5_SK BETWEEN :GSI5_SK_FROM AND :GSI5_SK_TO"
        expression_values = {
            ":GSI5_PK": {"S": 'METADATA_TRANSACTION'},
            ":GSI5_SK_FROM": {"S": f"branch_id{branch_id}#terminal_id{terminal_id}#created_at{from_date}"},
            ":GSI5_SK_TO": {"S": f"branch_id{branch_id}#terminal_id{terminal_id}#created_at{to_date}"},
        }
        
        resp = client.query(
            TableName=table_name,
            IndexName='GSI5',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items

    @staticmethod
    @filterAttrItemRes
    def get_trans_by_branch_and_terminal_and_mode(branch_id,terminal_id,tran_mode,from_date,to_date):
        key_condition_expression = "PK = :PK and SK BETWEEN :SK_FROM AND :SK_TO"
        expression_values = {
            ":PK": {"S": 'Transaction'},
            ":SK_FROM": {"S": f"branch_id{branch_id}#terminal_id{terminal_id}#tran_mode{tran_mode}#created_at{from_date}"},
            ":SK_TO": {"S": f"branch_id{branch_id}#terminal_id{terminal_id}#tran_mode{tran_mode}#created_at{to_date}"},
        }
        resp = client.query(
            TableName=table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_values
        )
        Items = resp.get('Items')
        return Items


if __name__ =="__main__":
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
            "currency":"currency1",
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
                "kyc_document": ['kyc_document1','kyc_document2']
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
            "teller_id":"teller_id1",
            "sender":customers[0]['customer_id'],
            "receiver":customers[1]['customer_id'],
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
            # "tendering": {
            #     "payment_mode": {
            #         "cash": "11",
            #         "nets": "2",
            #         "dbs": "2",
            #         "maybank": "2",
            #         "banktransfer": "2",
            #         "total": "19"
            #     },
            #     "source_amount": "1111",
            #     "fees": 10,
            #     "promo": {
            #         "percent_dollar": 0,
            #         "amount": 3
            #     },
            #     "status": "paid"
            # },
            "bank_detail":banks[0]["bank_id"],
            "terminal_id":terminals[0]["terminal_id"],
            "branch_id":branches[0]["branch_id"],
            "payment_id":payments[0]["payment_id"],
            "created_at":"2022-08-17T03:03:59.952Z"
        }
    ]


    for item in payments:
        instance = Payment(**item)
        instance.insert()
    
    for item in service_types:
        instance = ServiceType(**item)
        instance.insert()
    
    for item in promo_codes:
        instance = PromoCode(**item)
        instance.insert()
    
    for item in terminals:
        instance = Terminal(**item)
        instance.insert()

    for item in branches:
        instance = Branch(**item)
        instance.insert()

    for item in tellers:
        instance = Teller(**item)
        instance.insert()
    
    for item in stocks:
        instance = Stock(**item)
        instance.insert()

    for item in customers:
        instance = Customer(**item)
        instance.insert()

    for item in banks:
        instance = Bank(**item)
        instance.insert()

    for item in transactions:
        instance = Transaction(**item)
        instance.insert()

