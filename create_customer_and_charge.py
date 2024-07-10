import openpay

# Set up your Openpay credentials
openpay.api_key = 'sk_d5652f7a4e3f47cca1f4bea6c257a772'
openpay.verify_ssl_certs = False
openpay.merchant_id = 'mssr4offqy1nyyqlkx5e'
openpay.production = False  # Use True for production environment

# Create a customer
customer_data = {
    'name': 'John',
    'last_name': 'Doe',
    'phone_number': '5555555555',
    'email': 'john.doe@example.com',
    'requires_account': False
}

try:
    customer = openpay.Customer.create(**customer_data)
    customer_id = customer['id']
    print(f"Customer created: {customer}")
except openpay.error.OpenpayError as e:
    print(f"Error creating customer: {e}")
    customer_id = None

# Create a charge with the customer ID, if customer creation was successful
if customer_id:
    charge_data = {
        'method': 'bank_account',
        'amount': 200.00,
        'description': 'Bank transfer charge',
        'order_id': 'order-id-00052',
        'customer': customer_id  # Use the customer ID directly
    }

    try:
        charge = openpay.Charge.create(**charge_data)
        print(f"Charge created: {charge}")
    except openpay.error.OpenpayError as e:
        print(f"Error creating charge: {e}")
