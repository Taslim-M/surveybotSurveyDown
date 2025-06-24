from supabase_handler import SupabaseHandler
import json

def test_star_wars_table():
    """Test reading from the StarWarsTest1 table"""
    try:
        # Initialize database handler
        print("Initializing database handler...")
        db_handler = SupabaseHandler()
        
        # Test connection by reading from StarWarsTest1 table
        print("Reading from StarWarsTest1 table...")
        data = db_handler.read_star_wars_test1()
        
        if data:
            print(f"\nFound {len(data)} rows in StarWarsTest1 table:")
            print("=" * 50)
            
            # Display the data in a formatted way
            for i, row in enumerate(data, 1):
                print(f"\nRow {i}:")
                for key, value in row.items():
                    print(f"  {key}: {value}")
                print("-" * 30)
            
            # Also save to JSON file for easier viewing
            with open('star_wars_data.json', 'w') as f:
                json.dump(data, f, indent=2, default=str)
            print(f"\nData also saved to 'star_wars_data.json'")
            
        else:
            print("No data found in StarWarsTest1 table or table doesn't exist.")
            
    except Exception as e:
        print(f"Error testing StarWarsTest1 table: {e}")
        print("\nMake sure you have:")
        print("1. Created a .env file with your Supabase credentials")
        print("2. The StarWarsTest1 table exists in your database")
        print("3. Your database connection is working")

if __name__ == "__main__":
    test_star_wars_table() 