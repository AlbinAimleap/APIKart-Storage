import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(".env"))

class Config:
    DB_TYPE = os.getenv('DB_ENGINE', 'sqlite')
    DB_NAME = os.getenv('DB_NAME', 'example.db')
    DB_USER = os.getenv('DB_USER', '')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    

    @property
    def database_url(self):
        """Construct the appropriate database URL based on the selected database type."""
        if self.DB_TYPE == 'postgres':
            return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        
        elif self.DB_TYPE == 'mysql':
            return f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        
        elif self.DB_TYPE == 'sqlite':
            return f"sqlite:///{self.DB_NAME}"
        
        else:
            raise ValueError("Unsupported database type")


config = Config()
DATABASE_URL = config.database_url
