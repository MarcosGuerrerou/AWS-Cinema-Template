"""
Module: BaseDB
Class: BaseDynamoDBAbstraction

Description: Basic DynamoDB table operations that can be used and adapted as the table requires.

Imports:
    - typing: For type hinting.
    - decimal.Decimal: For type conversion supported by DynamoDB.
    - Exceptions: Exceptions collection for API responses.
    - boto3: To initialize Table and DynamoDB resource instances.
    - re: Regular expression matching for Vehicle Identification Numbers.
    - boto3.dynamodb.types.Binary: Binary representation for DynamoDB.
    - json: JSON parsing and dumping.
"""
import logging
from typing import Tuple, List, Union
from decimal import Decimal

try:
    from Exceptions import *
except ImportError:
    from src.api.Exceptions import *

DYNAMODB_KEY_TYPES = Union[str, int, Decimal]

class BaseDynamoDBAbstraction(object):
    """
    BaseDynamoDBAbstraction
    ### Initialization parameters:
        - client (botocore.client.DynamoDB): Boto3 initialized client for DynamoDB.
        - table_name (str): Name of the table operations will be performed on.
        - [Optional] verbose (bool): Whether to print operations and results.
        - [Optional] debug (bool): Whether to perform write operations.

    ### Methods:
        - get_by_keys(): Fetch a single item from either a simple or complex primary key.
        - batch_get_by_key(): Fetch a list of items from list(s) of primary keys.
        - query_table(): Pass-through of a query operation.
        - scan_table(): Returns the full table. Does not support pagination.
        - add_item(): Insert an item into the table. It will handle dtype conversion.
        - update_item(): Update a single attribute on a table item.
        - dump_item(): Delete an item from the table.

    ### Pending to implement:
        - Pagination for scan_table() method.                  
            
    """

    def __init__(self, client, table_name: str, verbose = False, debug = False):
        import boto3

        self.verbose = verbose
        self.debug = debug

        self.client = client
        self.resource = boto3.resource('dynamodb')

        self.table = self.resource.Table(table_name)
        self.key_schema = self.table.key_schema
        
        self.partition_key = self.key_schema[0]['AttributeName']

        if len(self.key_schema) == 2:
            self.sort_key = self.key_schema[1]['AttributeName']
    
    """

     READ OPERATIONS
     
    """
    def get_by_keys( self,
                    pk_value: DYNAMODB_KEY_TYPES,
                    sk_value: DYNAMODB_KEY_TYPES = None,
                    AttributesToGet: List[str] =  []
                    ) -> Tuple[dict, int]:
        """Retrieve a single dynamodb item via the primary key (partition key + sort key)

        Args:
            pk_value (DYNAMODB_KEY_TYPES): Partition key value
            sk_value (DYNAMODB_KEY_TYPES, optional): Sort key value. Defaults to None, if the table you're accessing has a sort key and it was not given it will raise a MissingParameter exception.
            AttributesToGet (List[str], optional): List of attributes to retrieve. Defaults to [].

        Raises:
            NotFoundError: In case item is not found

        Returns:
            payload (dict): Dictionary containing the item in the 'data' key, and any errors in the 'errors' key
            statusCode (int): HTTP status code of the database response
        """
        arguments = self.define_key_args(pk_value, sk_value)

        if AttributesToGet:
            arguments['AttributesToGet'] = AttributesToGet

        response = self.table.get_item(**arguments)

        statusCode = response['ResponseMetadata']['HTTPStatusCode']

        payload = {}

        try:
            payload['data'] = BaseDynamoDBAbstraction.convert_dtypes(response['Item'], "dynamo2native")
        except KeyError:
            raise NotFoundError(f'Item {pk_value}{sk_value} was not found')
        
        return payload, statusCode
    
    def batch_get_by_key(self,
                         pk_list: List[DYNAMODB_KEY_TYPES],
                         sk_list: List[DYNAMODB_KEY_TYPES] = None,
                         AttributesToGet: List[str] =  []
                        ) -> Tuple[dict, int]:
    
        arguments = self.define_list_key_args(pk_list, sk_list)

        if AttributesToGet:
            arguments['RequestItems'][self.table.name]['AttributesToGet'] = AttributesToGet

        response = self.client.batch_get_item(**arguments)

        statusCode = response['ResponseMetadata']['HTTPStatusCode']

        payload = {}
        
        payload['data'] = response['Responses'][self.table.name]
        payload['data'] = BaseDynamoDBAbstraction.infer_types(payload['data'])

        if not payload['data']:
            raise NotFoundError("None of the items were found")

        if response['UnprocessedKeys']:
            payload['error'] = response['UnprocessedKeys'][self.table.name]['Keys']
        
        return payload, statusCode
    
    def query_table(self, type: str, **kwargs):

        payload = {}

        if type == 'PartiQL':
            response = self.client.execute_statement(**kwargs)

        elif type == 'native':
            response = self.table.query(**kwargs)
            if response['Count'] == 0:
                raise NotFoundError("None of the given keys were found")
        
        statusCode = response['ResponseMetadata']['HTTPStatusCode']
        payload['data'] = response['Items']

        return payload, statusCode
    
    def scan_table(self, **kwargs):
        self.payload = {}
        
        response = self.table.scan()
        statusCode = response['ResponseMetadata']['HTTPStatusCode']

        Items = response['Items']

        return Items, statusCode
    
    """

     WRITE OPERATIONS

    """
 
    def add_item(self,
                 pk_value: DYNAMODB_KEY_TYPES,
                 sk_value: DYNAMODB_KEY_TYPES = None,
                 item: dict = {}
                 ) -> Tuple[bool, int]:
        
        key_args = self.define_key_args(pk_value, sk_value) 

        for k,v in key_args['Key'].items():
            item[k] = v
        
        item = BaseDynamoDBAbstraction.convert_dtypes(item, "native2dynamo")

        response = self.table.put_item(Item = item)

        statusCode = response['ResponseMetadata']['HTTPStatusCode']
        success = True if statusCode == 200 else False

        return success, statusCode
    
    def update_item(self,
                    pk_value: DYNAMODB_KEY_TYPES,
                    sk_value: DYNAMODB_KEY_TYPES = None,
                    attribute_name: str = None,
                    attribute_value: str = None,
                    **kwargs
                    )-> Tuple[bool, int]:
        from botocore.exceptions import ClientError
        
        payload = {}

        arguments = self.define_key_args(pk_value, sk_value)
        arguments['UpdateExpression'] = f'SET {attribute_name} = :r'
        arguments['ExpressionAttributeValues'] = {':r': attribute_value}
        arguments['ReturnValues'] = kwargs.get('ReturnValues', 'UPDATED_OLD')
        arguments['ConditionExpression'] = kwargs.get('ConditionExpression', f'attribute_exists({attribute_name})')

        try:
            response = self.table.update_item(**arguments)
        except ClientError as e:
            logging.exception(e)
            raise InvalidParameter("Check that the attribute value is not empty")

        statusCode = response['ResponseMetadata']['HTTPStatusCode']
        if 'Attributes' in response:
            payload['data'] = response['Attributes']

            lookup = {'UPDATED_OLD': 'Returned only updated old item', 'ALL_OLD': 'Returned all old item', 'UPDATED_NEW': 'Returned only updated new item', 'ALL_NEW': 'Returned all new item'}

            payload['message'] = lookup[arguments['ReturnValues']]
        else:
            payload['message'] = 'Item was updated'

        return payload, statusCode 
    
    """

     DELETE OPERATIONS
     
    """

    def dump_item(self, pk_value: DYNAMODB_KEY_TYPES, sk_value: DYNAMODB_KEY_TYPES = None, **kwargs) -> Tuple[bool, int]:
        arguments = self.define_key_args(pk_value, sk_value)

        response = self.table.delete_item(**arguments)

        statusCode = response['ResponseMetadata']['HTTPStatusCode']

        success = True if statusCode == 200 else False

        return success, statusCode
    

    """
    Helper functions
    """
    def define_key_args(self,
                        pk_value: Union[List[DYNAMODB_KEY_TYPES], DYNAMODB_KEY_TYPES],
                        sk_value: Union[List[DYNAMODB_KEY_TYPES], DYNAMODB_KEY_TYPES] = None
                        )-> dict:
        """_summary_

        Args:
            pk_value (Union[List[DYNAMODB_KEY_TYPES], DYNAMODB_KEY_TYPES]): _description_
            sk_value (Union[List[DYNAMODB_KEY_TYPES], DYNAMODB_KEY_TYPES], optional): _description_. Defaults to None.

        Raises:
            MissingParameter: In case the table you're accessing has a sort key and it was not given it will raise a MissingParameter exception.

        Returns:
            dict: Return dictionary in the boto3 required format
        """
        
        arguments = {'Key': {self.partition_key: pk_value}}

        if len(self.key_schema) == 2:
            if sk_value == None:
                raise MissingParameter("Table requires a sort key value but none was provided")
            
            arguments['Key'][self.sort_key] = sk_value
        
        return arguments
    
    def define_list_key_args(self,
                            pk_list: List[DYNAMODB_KEY_TYPES],
                            sk_list: List[DYNAMODB_KEY_TYPES] = None):

        if len(self.key_schema) == 2:
            if len(sk_list) != len(pk_list):
                raise ValueError("The number of primary keys and sort keys must be the same")
                
            arguments = {'RequestItems': {self.table.name: {'Keys': [{self.partition_key: pk_val, self.sort_key: sk_val} for pk_val, sk_val in zip(pk_list, sk_list)]}}}
        else:
            arguments = {'RequestItems': {self.table.name: {'Keys': [{self.partition_key: pk_val} for pk_val in pk_list]}}}
        
        return arguments
    
    @staticmethod
    def convert_dtypes(item: dict, type: str)-> dict:
        """Convert a dictionary retrieved from dynamodb to native python datatypes"""
        from decimal import Decimal
        from boto3.dynamodb.types import Binary

        allowed_types = ["dynamo2native", "native2dynamo"]

        if type not in allowed_types:
            raise ValueError(f"Invalid type of conversion: {type}")
        
        
        if type == "dynamo2native":
            for k, v in item.items():
                if isinstance(v, Binary):
                    item[k] = v.value
                elif isinstance(v, Decimal):
                    item[k] = float(v)
        elif type == "native2dynamo":
            for k, v in item.items():
                if isinstance(v, (bytes, bytearray)):
                    item[k] = Binary(bytes)
                elif isinstance(v, float):
                    item[k] = Decimal(v)   
    
        return item
    
    @staticmethod
    def infer_types(items: Union[list, dict]) -> Union[list, dict]:
        """Convert type explicit dynamodb dictionaries into native python types

        Args:
            items (Union[list, dict]): Item or list of items from DynamoDB in format:
                                    "{Attribute1: {'S': {'string_value'}, Attribute2: {'N': {'number_value'}}}}"

        Returns:
            Union[list, dict]: Return dictionary without redundant dictionaries.
        """
        import json
        if not isinstance(items, list):
            items = [items]

        conversions= {
            'N': lambda x: int(float(x)) if float(x) - int(float(x)) == 0 else float(x),
            'S': lambda x: x,
            'BOOL': lambda x: True if x in ('True', 'true', 'TRUE') else False,
            'NULL': lambda x: None,
            'B': lambda x: bytes(x),
            'L': lambda x: json.loads(x),
            'M': lambda x: json.loads(x),
            'NS': lambda x: json.loads(x),
            'SS': lambda x: json.loads(x)
        }
        for item in items:
            for attr_name, val_dict in item.items():
                for dtype, value in val_dict.items():
                    item[attr_name] = conversions[dtype](value)
        
        return items