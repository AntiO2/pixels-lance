"""Example usage of Pixels Lance with different benchmark schemas"""

from pathlib import Path
from pixels_lance import RpcFetcher, DataParser, LanceDBStore
from pixels_lance.config import ConfigManager


def example_customer_benchmark():
    """Process customer table benchmark"""
    config = ConfigManager("config/config.yaml").get()

    # Use customer schema
    parser = DataParser(schema_path="config/schema_customer.yaml")
    store = LanceDBStore(config=config.lancedb)

    # Create table for customer data
    store.create_table(table_name="customer")

    # Example: Parse binary data
    # binary_data = fetcher.fetch("get_customer", {"id": 1})
    # if binary_data:
    #     parsed = parser.parse(binary_data)
    #     store.save(parsed, table_name="customer")

    print("Customer benchmark example")


def example_company_benchmark():
    """Process company table benchmark"""
    config = ConfigManager("config/config.yaml").get()

    parser = DataParser(schema_path="config/schema_company.yaml")
    store = LanceDBStore(config=config.lancedb)
    store.create_table(table_name="company")

    print("Company benchmark example")


def example_transfer_benchmark():
    """Process transfer table benchmark"""
    config = ConfigManager("config/config.yaml").get()

    parser = DataParser(schema_path="config/schema_transfer.yaml")
    store = LanceDBStore(config=config.lancedb)
    store.create_table(table_name="transfer")

    print("Transfer benchmark example")


def example_account_benchmarks():
    """Process account-related benchmarks"""
    config = ConfigManager("config/config.yaml").get()
    store = LanceDBStore(config=config.lancedb)

    # Saving Account
    parser = DataParser(schema_path="config/schema_savingAccount.yaml")
    store.create_table(table_name="savingAccount")

    # Checking Account
    parser = DataParser(schema_path="config/schema_checkingAccount.yaml")
    store.create_table(table_name="checkingAccount")

    print("Account benchmarks example")


def example_loan_benchmarks():
    """Process loan-related benchmarks"""
    config = ConfigManager("config/config.yaml").get()
    store = LanceDBStore(config=config.lancedb)

    # Loan applications
    parser = DataParser(schema_path="config/schema_loanapps.yaml")
    store.create_table(table_name="loanapps")

    # Loan transactions
    parser = DataParser(schema_path="config/schema_loantrans.yaml")
    store.create_table(table_name="loantrans")

    print("Loan benchmarks example")


def example_batch_processing():
    """Batch processing example with schema"""
    config = ConfigManager("config/config.yaml").get()

    parser = DataParser(schema_path="config/schema_customer.yaml")
    store = LanceDBStore(config=config.lancedb)

    # Process multiple records
    batch_data = []
    # for i in range(config.batch_size):
    #     data = fetcher.fetch("get_customer", {"id": i})
    #     if data:
    #         parsed = parser.parse(data)
    #         batch_data.append(parsed)

    # Save batch
    if batch_data:
        store.save(batch_data, table_name="customer")
        print(f"Saved {len(batch_data)} customer records")


def example_parse_binary():
    """Example of parsing binary data directly"""
    from pixels_lance.parser import DataParser, Schema, SchemaField

    # Create a simple schema
    fields = [
        SchemaField("id", "int32", offset=0),
        SchemaField("amount", "float32", offset=4),
        SchemaField("name", "varchar", size=20, offset=8),
    ]
    schema = Schema(fields)
    parser = DataParser(schema=schema)

    # Parse some binary data
    # binary_data = b'...actual binary data...'
    # result = parser.parse(binary_data)
    # print(result)

    print("Binary parsing example")


if __name__ == "__main__":
    print("Pixels Lance Examples\n")
    print("=" * 50)

    try:
        example_customer_benchmark()
        example_company_benchmark()
        example_transfer_benchmark()
        example_account_benchmarks()
        example_loan_benchmarks()
        example_parse_binary()

        print("\n" + "=" * 50)
        print("\nNote: To actually fetch and parse data:")
        print("1. Configure RPC URL in config/config.yaml")
        print("2. Implement RPC method calls to fetch binary data")
        print("3. Uncomment the data fetching code in examples above")
        print("4. Run: python examples.py")

    except Exception as e:
        print(f"Error: {e}")

