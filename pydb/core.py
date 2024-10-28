import typing
from typing import Type, Dict, ClassVar, List, Optional, Any, TypeVar, get_origin, get_args, Union
from sqlalchemy import Column, Integer, String, Boolean, JSON, Float, Interval, Date, Time, DateTime
from datetime import datetime, date, time
from sqlalchemy.orm import Session
from pydantic import BaseModel as PydanticBaseModel, ValidationError
from pydb import SessionLocal, Base, init_db
import inspect
from pydb.logger import logger
from pydb.config import Config
import json
from dotenv import load_dotenv, find_dotenv


from typing import TypeVar

created_models: Dict[str, Type[Base]] = {}
T = TypeVar('T', bound='BaseSchema')

class BaseSchema(PydanticBaseModel):
    """Base Pydantic model with CRUD methods."""
    id: ClassVar[int] = None
    
    class Config:
        from_attributes = True
        populate_by_name = True
        use_enum_values = True
        validate_assignment = True
        arbitrary_types_allowed = True
        json_encoders = {
            set: list,
            frozenset: list,
            bytes: lambda v: v.decode(),
        }
    
    def __init__(self, **data: Any):
        super().__init__(**data)
        self.init_db()
    
    
    @classmethod
    @property
    def get_current_database(cls) -> Optional[str]:
        """Get the current database name directly from the session."""
        session = SessionLocal()
        try:
            # Extract the database name directly from the session's bind
            return session.bind.url.database
        except Exception as e:
            logger.error(f"Error getting current database name: {str(e)}")
        finally:
            session.close()
    
    
    @classmethod
    def init_db(cls) -> None:
        """Create the corresponding table in the database."""
        model_class = create_sqlalchemy_model_from_pydantic(cls)
        Base.metadata.create_all(bind=SessionLocal().bind)
        logger.info(f"Get or Create Table: {model_class.__tablename__}")

    def save(self) -> None:
        """Save the current instance to the database."""
        session = SessionLocal()
        try:
            model_class = create_sqlalchemy_model_from_pydantic(self.__class__)
            model_data = self.model_dump()
            model_instance = model_class(**model_data)
            session.add(model_instance)
            session.commit()
            logger.info(f"Saved to {model_instance.__tablename__}")
            return model_instance.id
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving {self.__class__.__name__}: {str(e)}")
        finally:
            session.close()   
                        
    @classmethod
    def query(cls):
        """Return a query object for the model."""
        session = SessionLocal()
        model_class = create_sqlalchemy_model_from_pydantic(cls)
        return session.query(model_class)    
    
    @classmethod
    def get_by_id(cls, id: int) -> Optional["BaseSchema"]:
        """Retrieve an instance by its ID."""
        session = SessionLocal()
        try:
            model_class = create_sqlalchemy_model_from_pydantic(cls)
            instance = session.query(model_class).filter(model_class.id == id).first()
            if instance:
                return cls.from_orm(instance) 
            return None
        finally:
            session.close()
    
    @classmethod
    def update(cls, id: int, **kwargs: Any) -> Optional["BaseSchema"]:
        """Update an instance by its ID."""
        session = SessionLocal()
        try:
            model_class = create_sqlalchemy_model_from_pydantic(cls)
            instance = session.query(model_class).filter(model_class.id == id).first()
            if instance:
                for key, value in kwargs.items():
                    setattr(instance, key, value)
                session.commit()
                return cls.from_orm(instance)
            return None
        except Exception as e:
            session.rollback()
            print("Error updating instance:", e)
            return None
        finally:
            session.close()

    @classmethod
    def delete(cls, id: int) -> bool:
        """Delete an instance by its ID."""
        session = SessionLocal()
        try:
            model_class = create_sqlalchemy_model_from_pydantic(cls)
            instance = session.query(model_class).filter(model_class.id == id).first()
            if instance:
                session.delete(instance)
                session.commit()
                print(f"{cls.__name__} with ID {id} deleted.")
                return True
            print(f"{cls.__name__} with ID {id} not found.")
            return False
        except Exception as e:
            session.rollback()
            print("Error deleting instance:", e)
            return False
        finally:
            session.close()

    @classmethod
    def get_all(cls) -> List["BaseSchema"]:
        """Retrieve all instances of the model."""
        session = SessionLocal()
        try:
            model_class = create_sqlalchemy_model_from_pydantic(cls)
            instances = session.query(model_class).all()
            return [cls.from_orm(instance) for instance in instances]
        finally:
            session.close()

def create_sqlalchemy_model_from_pydantic(pydantic_model: Type[T]) -> Type[Base]:
    """Creates an SQLAlchemy model class from a Pydantic model."""
    
    model_class_name = pydantic_model.__name__ + 'Model'
    
    if model_class_name in created_models:
        return created_models[model_class_name]
    
    attributes = {
        "__tablename__": pydantic_model.__name__.lower(),
        "id": Column(Integer, primary_key=True, autoincrement=True),
    }

    for field_name, field_type in pydantic_model.__annotations__.items():
        field_info = pydantic_model.model_fields[field_name]
        
        if field_name == 'id':
            continue
        
        nullable = get_origin(field_type) is Union and type(None) in get_args(field_type)
        if inspect.isclass(field_type) and issubclass(field_type, PydanticBaseModel):
            # Handle relationship to another model
            print("Found Relationnship")
            related_model_class = create_sqlalchemy_model_from_pydantic(field_type)
            related_model_class.save()
            # Create a foreign key column for the related model
            attributes[field_name + "_id"] = Column(Integer, ForeignKey(f"{related_model_class.__tablename__}.id"))
            attributes[field_name] = relationship(related_model_class.__name__, back_populates=f"{field_name}_backref")
            
        elif field_type in [bool, typing.Optional[bool]]:
            column_type = Boolean
        elif field_type in [str, typing.Optional[str]]:
            column_type = String
        elif field_type in [int, typing.Optional[int]]:
            column_type = Integer
        elif field_type in [float, typing.Optional[float]]:
            column_type = Float
        elif field_type in [bool, typing.Optional[Date]]:
            column_type = Date
        elif field_type in [bool, typing.Optional[Time]]:
            column_type = Time
        elif field_type in [bool, typing.Optional[DateTime]]:
            column_type = DateTime
        else:
            column_type = String
            
        unique = False
        if extra:= field_info.json_schema_extra:
            unique = extra.get("unique")
        
        attributes[field_name] = Column(column_type, nullable=nullable, unique=unique)

    model_class = type(model_class_name, (Base,), attributes)
    created_models[model_class_name] = model_class
    return model_class