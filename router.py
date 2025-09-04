'''                                                                                                
   _|_|_|  _|_|_|_|  _|      _|  _|_|_|_|  _|_|_|    _|_|_|    _|_|_|      _|_|_|    _|_|_|        
 _|        _|        _|_|    _|  _|        _|    _|    _|    _|            _|    _|  _|    _|      
 _|  _|_|  _|_|_|    _|  _|  _|  _|_|_|    _|_|_|      _|    _|            _|    _|  _|_|_|        
 _|    _|  _|        _|    _|_|  _|        _|    _|    _|    _|            _|    _|  _|    _|      
   _|_|_|  _|_|_|_|  _|      _|  _|_|_|_|  _|    _|  _|_|_|    _|_|_|      _|_|_|    _|_|_|        
                                                                                                                                                                                                      
'''
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

from typing import Optional, Dict, Any

from logger import get_logger

logger = get_logger('MFExtractor')

class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors"""
    pass

def validate_required_env_vars(db_type: str, required_vars: list) -> None:
    """
        Validate that all required environment variables are set
    """

    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        raise DatabaseConnectionError(
            f"Missing required environment variables for {db_type}: {', '.join(missing_vars)}"
        )

def get_database_url(db_type: str) -> str:
    """
        Generate a SQLAlchemy-compatible database URL based on environment variables.

        Supported db_type values:
        - sqlite
        - postgres
        - mysql
        - oracle
        - snowflake
        - mssql (SQL Server)
        - database (full URL override)

        Environment variables are validated before creating the connection string.
    """
    db = db_type.lower()

    try:
        if db == "database":
            validate_required_env_vars("database", ["DATABASE_URL"])
            return os.environ["DATABASE_URL"]

        elif db == "sqlite":
            path = os.environ.get("SQLITE_PATH", "example.db")
            db_dir = os.path.dirname(path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            return f"sqlite:///{path}"

        elif db == "postgres" or db == "postgresql":
            validate_required_env_vars("postgres", ["PGUSER", "PGPASSWORD", "PGDATABASE"])
            user = quote_plus(os.environ["PGUSER"])
            pwd = quote_plus(os.environ["PGPASSWORD"])
            host = os.environ.get("PGHOST", "localhost")
            port = os.environ.get("PGPORT", "5432")
            name = os.environ["PGDATABASE"]
            sslmode = os.environ.get("PGSSLMODE", "prefer")
            url = f"postgresql://{user}:{pwd}@{host}:{port}/{name}"
            if sslmode != "disable":
                url += f"?sslmode={sslmode}"
            return url

        elif db == "mysql":
            validate_required_env_vars("mysql", ["MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DB"])
            user = quote_plus(os.environ["MYSQL_USER"])
            pwd = quote_plus(os.environ["MYSQL_PASSWORD"])
            host = os.environ.get("MYSQL_HOST", "localhost")
            port = os.environ.get("MYSQL_PORT", "3306")
            name = os.environ["MYSQL_DB"]
            charset = os.environ.get("MYSQL_CHARSET", "utf8mb4")
            ssl_disabled = os.environ.get("MYSQL_SSL_DISABLED", "false").lower() == "true"
            
            url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}?charset={charset}"
            if ssl_disabled:
                url += "&ssl_disabled=true"
            return url

        elif db == "oracle":
            validate_required_env_vars("oracle", ["ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_SID"])
            user = quote_plus(os.environ["ORACLE_USER"])
            pwd = quote_plus(os.environ["ORACLE_PASSWORD"])
            host = os.environ.get("ORACLE_HOST", "localhost")
            port = os.environ.get("ORACLE_PORT", "1521")
            sid = os.environ["ORACLE_SID"]
            service_name = os.environ.get("ORACLE_SERVICE_NAME")
            if service_name:
                return f"oracle+cx_oracle://{user}:{pwd}@{host}:{port}/?service_name={service_name}"
            else:
                return f"oracle+cx_oracle://{user}:{pwd}@{host}:{port}/{sid}"

        elif db == "snowflake":
            validate_required_env_vars("snowflake", ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT"])
            user = quote_plus(os.environ["SNOWFLAKE_USER"])
            pwd = quote_plus(os.environ["SNOWFLAKE_PASSWORD"])
            account = os.environ["SNOWFLAKE_ACCOUNT"]
            warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
            database = os.environ.get("SNOWFLAKE_DATABASE")
            schema = os.environ.get("SNOWFLAKE_SCHEMA")
            role = os.environ.get("SNOWFLAKE_ROLE")
            path = ""
            if database:
                path += f"/{database}"
                if schema:
                    path += f"/{schema}"
            params = []
            if warehouse:
                params.append(f"warehouse={warehouse}")
            if role:
                params.append(f"role={role}")
            
            url = f"snowflake://{user}:{pwd}@{account}{path}"
            if params:
                url += f"?{'&'.join(params)}"
            return url

        elif db == "mssql" or db == "sqlserver":
            validate_required_env_vars("mssql", ["MSSQL_USER", "MSSQL_PASSWORD", "MSSQL_DATABASE"])
            user = quote_plus(os.environ["MSSQL_USER"])
            pwd = quote_plus(os.environ["MSSQL_PASSWORD"])
            host = os.environ.get("MSSQL_HOST", "localhost")
            port = os.environ.get("MSSQL_PORT", "1433")
            database = os.environ["MSSQL_DATABASE"]
            driver = os.environ.get("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
            
            return f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{database}?driver={quote_plus(driver)}"

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    except KeyError as e:
        raise DatabaseConnectionError(f"Missing environment variable: {str(e)}")
    except Exception as e:
        raise DatabaseConnectionError(f"Error creating database URL for {db_type}: {str(e)}")


def get_engine(db_type: str, **engine_kwargs) -> Engine:
    """
        Create and return a SQLAlchemy Engine for the given database type.
        Additional keyword args are passed to create_engine().
        
        Default engine configurations are applied based on database type.
    """
    url = get_database_url(db_type)

    default_configs = {
        'postgres': {
            'pool_size': 10,
            'max_overflow': 20,
            'pool_pre_ping': True,
            'pool_recycle': 3600,
        },
        'mysql': {
            'pool_size': 10,
            'max_overflow': 20,
            'pool_pre_ping': True,
            'pool_recycle': 3600,
        },
        'oracle': {
            'pool_size': 5,
            'max_overflow': 10,
            'pool_pre_ping': True,
            'pool_recycle': 3600,
        },
        'snowflake': {
            'pool_size': 5,
            'max_overflow': 10,
            'pool_pre_ping': True,
        },
        'mssql': {
            'pool_size': 10,
            'max_overflow': 20,
            'pool_pre_ping': True,
            'pool_recycle': 3600,
        },
        'sqlite': {
            'pool_pre_ping': False,  
        }
    }
    
    db_key = db_type.lower()
    if db_key in ['postgres', 'postgresql']:
        db_key = 'postgres'
    elif db_key in ['mssql', 'sqlserver']:
        db_key = 'mssql'
    
    config = default_configs.get(db_key, {})
    config.update(engine_kwargs)  
    
    try:
        engine = create_engine(url, **config)
        logger.debug(f"Successfully created {db_type} engine")
        return engine
    except Exception as e:
        raise DatabaseConnectionError(f"Failed to create engine for {db_type}: {str(e)}")


def get_session(db_type: str, engine_kwargs: Optional[Dict[str, Any]] = None, **session_kwargs):
    """
        Create and return a SQLAlchemy Session for the given database type.
        
        Args:
            db_type: Database type string
            engine_kwargs: Dictionary of kwargs to pass to create_engine()
            **session_kwargs: Additional kwargs passed to sessionmaker()
    """

    engine_kwargs = engine_kwargs or {}
    engine = get_engine(db_type, **engine_kwargs)
    
    session_config = {
        'autocommit': False,
        'autoflush': True,
        'bind': engine
    }
    session_config.update(session_kwargs)
    
    try:
        Session = sessionmaker(**session_config)
        logger.debug(f"Successfully created {db_type} session")
        return Session
    except Exception as e:
        raise DatabaseConnectionError(f"Failed to create session for {db_type}: {str(e)}")


def test_connection(db_type: str) -> bool:
    """
        Test database connection by creating an engine and executing a simple query.
        
        Returns:
            bool: True if connection successful, False otherwise
    """
    try:
        engine = get_engine(db_type)
        with engine.connect() as conn:
            result = conn.execute("SELECT 1").fetchone()
            logger.debug(f"Connection test successful for {db_type}")
            return True
    except Exception as e:
        logger.error(f"Connection test failed for {db_type}: {str(e)}")
        return False


def get_connection_info(db_type: str) -> Dict[str, str]:
    """
        Return connection information for debugging (without sensitive data)
    """
    db = db_type.lower()
    info = {"db_type": db_type}
    
    if db == "postgres":
        info.update({
            "host": os.environ.get("PGHOST", "localhost"),
            "port": os.environ.get("PGPORT", "5432"),
            "database": os.environ.get("PGDATABASE", "N/A"),
            "user": os.environ.get("PGUSER", "N/A")
        })
    elif db == "mysql":
        info.update({
            "host": os.environ.get("MYSQL_HOST", "localhost"),
            "port": os.environ.get("MYSQL_PORT", "3306"),
            "database": os.environ.get("MYSQL_DB", "N/A"),
            "user": os.environ.get("MYSQL_USER", "N/A")
        })
    elif db == "oracle":
        info.update({
            "host": os.environ.get("ORACLE_HOST", "localhost"),
            "port": os.environ.get("ORACLE_PORT", "1521"),
            "sid": os.environ.get("ORACLE_SID", "N/A"),
            "user": os.environ.get("ORACLE_USER", "N/A")
        })
    elif db == "snowflake":
        info.update({
            "account": os.environ.get("SNOWFLAKE_ACCOUNT", "N/A"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "N/A"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA", "N/A"),
            "user": os.environ.get("SNOWFLAKE_USER", "N/A")
        })
    elif db == "sqlite":
        info.update({
            "path": os.environ.get("SQLITE_PATH", "example.db")
        })
    elif db in ["mssql", "sqlserver"]:
        info.update({
            "host": os.environ.get("MSSQL_HOST", "localhost"),
            "port": os.environ.get("MSSQL_PORT", "1433"),
            "database": os.environ.get("MSSQL_DATABASE", "N/A"),
            "user": os.environ.get("MSSQL_USER", "N/A")
        })
    
    return info