from urllib.parse import quote_plus

_password = "NewStrongPassword@123"  

DB_CONFIG = {
    "host":     "localhost",
    "port":     "5432",
    "database": "nestkraft_export_intelligence",
    "username": "postgres",
    "password": _password
}

# quote_plus encodes @ and other special characters safely
CONNECTION_STRING = (
    f"postgresql+psycopg2://{DB_CONFIG['username']}:{quote_plus(_password)}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)