import os
from i_1_create_tenant import create_tenant
from i_2_generate_data import main as generate_data
from i_3_register_schema import main as register_schema
from i_4_validate_schema import main as validate_schema
from i_5_send_customers import main as send_customers
from i_6_send_events import main as send_events

def main():
    create_tenant()
    generate_data()
    register_schema()
    validate_schema()
    send_customers()
    send_events()

if __name__ == "__main__":
    main()