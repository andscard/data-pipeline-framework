# src/modules/ingestion/connectors/postgresql.py
"""
Conector para extraer datos desde PostgreSQL
"""

import pandas as pd
import psycopg2
from sqlalchemy import text
from sqlalchemy import create_engine
from typing import Optional, Dict, Any, List
import logging

from ..base import DataSourceConnector

logger = logging.getLogger(__name__)


class PostgreSQLConnector(DataSourceConnector):
    """
    Conector para extraer datos desde PostgreSQL.
    
    Soporta dos modos de conexión:
    - psycopg2: Para queries simples y rápidas
    - SQLAlchemy: Para queries complejas y ORM
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializar conector PostgreSQL.
        
        Args:
            config: Debe contener:
                - host: Hostname de PostgreSQL
                - port: Puerto (default: 5432)
                - database: Nombre de la base de datos
                - user: Usuario
                - password: Contraseña
                - mode: 'psycopg2' o 'sqlalchemy' (default: 'sqlalchemy')
        """
        super().__init__(config)
        self.mode = config.get("mode", "sqlalchemy")
        self.engine = None
    
    def connect(self) -> bool:
        """Establecer conexión con PostgreSQL"""
        try:
            if self.mode == "psycopg2":
                self.connection = psycopg2.connect(
                    host=self.config["host"],
                    port=self.config.get("port", 5432),
                    database=self.config["database"],
                    user=self.config["user"],
                    password=self.config["password"],
                    connect_timeout=10
                )
                logger.info(f"Connected to PostgreSQL via psycopg2: {self.config['host']}:{self.config.get('port', 5432)}")
            
            elif self.mode == "sqlalchemy":
                db_url = (
                    f"postgresql://{self.config['user']}:{self.config['password']}"
                    f"@{self.config['host']}:{self.config.get('port', 5432)}"
                    f"/{self.config['database']}"
                )
                self.engine = create_engine(db_url, pool_pre_ping=True)
                # Test connection
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    
                logger.info(f"Connected to PostgreSQL via SQLAlchemy: {self.config['host']}:{self.config.get('port', 5432)}")
            
            self.connected = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.connected = False
            return False
    
    def extract(self, query: Optional[str] = None, table: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Extraer datos desde PostgreSQL.
        
        Args:
            query: Query SQL personalizada
            table: Nombre de tabla (si no se proporciona query)
            **kwargs: Parámetros adicionales para pd.read_sql
            
        Returns:
            pd.DataFrame con los datos extraídos
        """
        if not self.connected:
            raise ConnectionError("Not connected to PostgreSQL. Call connect() first.")
        
        try:
            # Determinar la query a ejecutar
            if query:
                sql_query = query
            elif table:
                sql_query = f"SELECT * FROM {table}"
            else:
                raise ValueError("Must provide either 'query' or 'table' parameter")
            
            logger.info(f"Extracting data with query: {sql_query[:100]}...")
            
            # Extraer datos según el modo
            if self.mode == "psycopg2":
                df = pd.read_sql_query(sql_query, self.connection, **kwargs)
            elif self.mode == "sqlalchemy":
                df = pd.read_sql_query(sql_query, self.engine, **kwargs)
            
            self.log_extraction(len(df), success=True)
            logger.info(f"Extracted {len(df)} records, {len(df.columns)} columns")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from PostgreSQL: {e}")
            self.log_extraction(0, success=False)
            raise
    
    def extract_tables_list(self, schema: str = "public") -> List[str]:
        """
        Obtener lista de tablas en un esquema.
        
        Args:
            schema: Nombre del esquema (default: 'public')
            
        Returns:
            Lista de nombres de tablas
        """
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            ORDER BY table_name;
        """
        
        df = self.extract(query=query, params=(schema,))
        return df['table_name'].tolist()
    
    def extract_with_pagination(
        self, 
        query: str, 
        page_size: int = 10000
    ) -> pd.DataFrame:
        """
        Extraer datos grandes con paginación.
        
        Args:
            query: Query SQL base
            page_size: Tamaño de página
            
        Returns:
            pd.DataFrame con todos los datos
        """
        all_data = []
        offset = 0
        
        while True:
            paginated_query = f"{query} LIMIT {page_size} OFFSET {offset}"
            chunk = self.extract(query=paginated_query)
            
            if chunk.empty:
                break
            
            all_data.append(chunk)
            offset += page_size
            logger.info(f"Fetched page: offset={offset}, records={len(chunk)}")
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def validate_connection(self) -> bool:
        """Validar que la conexión está activa"""
        try:
            if self.mode == "psycopg2":
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
            elif self.mode == "sqlalchemy":
                with self.engine.connect() as conn:
                    conn.execute(sql.text("SELECT 1"))
            return True
        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            return False
    
    def close(self) -> None:
        """Cerrar conexión"""
        try:
            if self.mode == "psycopg2" and self.connection:
                self.connection.close()
            elif self.mode == "sqlalchemy" and self.engine:
                self.engine.dispose()
            
            self.connected = False
            logger.info("PostgreSQL connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")


# Factory function para facilitar creación
def create_postgres_connector(
    host: str = None,
    port: int = None,
    database: str = None,
    user: str = None,
    password: str = None,
    mode: str = "sqlalchemy"
) -> PostgreSQLConnector:
    """
    Factory function para crear un conector PostgreSQL.
    
    Lee automáticamente desde variables de entorno si no se proporcionan valores.
    
    Args:
        host: Hostname de PostgreSQL (default: lee de POSTGRES_HOST en .env)
        port: Puerto (default: lee de POSTGRES_PORT en .env)
        database: Nombre de la base de datos (default: lee de POSTGRES_DB en .env)
        user: Usuario (default: lee de POSTGRES_USER en .env)
        password: Contraseña (default: lee de POSTGRES_PASSWORD en .env)
        mode: 'psycopg2' o 'sqlalchemy'
        
    Returns:
        PostgreSQLConnector configurado
    """
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    # Usar valores de argumentos o leer de environment
    host = host or os.getenv('POSTGRES_HOST')
    port = port or (int(os.getenv('POSTGRES_PORT')) if os.getenv('POSTGRES_PORT') else None)
    database = database or os.getenv('POSTGRES_DB')
    user = user or os.getenv('POSTGRES_USER')
    password = password or os.getenv('POSTGRES_PASSWORD')
    
    # Validar que todos los valores estén presentes
    missing = []
    if not host: missing.append('host/POSTGRES_HOST')
    if not port: missing.append('port/POSTGRES_PORT')
    if not database: missing.append('database/POSTGRES_DB')
    if not user: missing.append('user/POSTGRES_USER')
    if not password: missing.append('password/POSTGRES_PASSWORD')
    
    if missing:
        raise ValueError(
            f"❌ Parámetros requeridos faltantes: {', '.join(missing)}\n"
            f"Proporciónalos como argumentos o defínelos en .env"
        )
    config = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "mode": mode
    }
    
    return PostgreSQLConnector(config)