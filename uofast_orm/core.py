"""
Simple ORM for U2 Unidata Files
================================

This module provides a simple ORM (Object-Relational Mapping) class that maps
dictionary items to Python objects and uses uopy File read_named_fields and
write_named_fields methods for CRUD operations.
"""

import logging
from typing import Any, Dict, List, Optional, Type, TypeVar
import uopy

logger = logging.getLogger("uofast-mcp.orm")

T = TypeVar('T', bound='UopyModel')


class UopyModel:
    """
    Base ORM class for mapping U2 file records to Python objects.

    This class provides CRUD operations using uopy File's read_named_fields
    and write_named_fields methods.

    Example (Simple - no mapping):
        >>> class Customer(UopyModel):
        ...     _file_name = "CUSTOMERS"
        ...     _field_names = ["NAME", "EMAIL", "PHONE", "ADDRESS"]
        ...
        >>> customer = Customer(session)
        >>> customer.data = {"NAME": "John Doe", "EMAIL": "john@example.com"}
        >>> customer.create("CUST001")

    Example (With property mapping):
        >>> class Customer(UopyModel):
        ...     _file_name = "CUSTOMERS"
        ...     _field_names = ["NAME", "EMAIL", "PHONE", "ADDRESS"]
        ...     _field_map = {
        ...         "full_name": "NAME",
        ...         "email_address": "EMAIL",
        ...         "phone_number": "PHONE",
        ...         "mailing_address": "ADDRESS"
        ...     }
        ...
        >>> customer = Customer(session)
        >>> customer.full_name = "John Doe"  # Maps to NAME field
        >>> customer.email_address = "john@example.com"  # Maps to EMAIL field
        >>> customer.create("CUST001")
    """

    # Class attributes to be overridden in subclasses
    _file_name: str = None  # Name of the U2 file
    _field_names: List[str] = []  # List of DICT field names to work with
    _field_map: Dict[str, str] = {}  # Optional: Maps property names to field names
    _enable_cache: bool = False  # Enable instance-level caching
    _cache_max_size: int = 100  # Maximum number of cached records

    # Class-level cache shared by all instances of the same model
    _record_cache: Dict[str, Dict[str, Any]] = {}

    def __init__(self, session: uopy.Session, record_id: Optional[str] = None, use_cache: bool = False):
        """
        Initialize the model.

        Args:
            session: Active uopy Session object
            record_id: Optional record ID if loading an existing record
            use_cache: Enable caching for this instance
        """
        if self._file_name is None:
            raise ValueError("_file_name must be defined in subclass")

        self.session = session
        self.record_id = record_id
        self.data: Dict[str, Any] = {}
        self._is_loaded = False
        self._use_cache = use_cache or self._enable_cache

        # Build reverse map for field name -> property name lookups
        self._reverse_field_map = {v: k for k, v in self._field_map.items()} if self._field_map else {}
        if self._field_names is None or len(self._field_names) == 0:
            if self._field_map:
                self._field_names = list(self._field_map.values())
            else:
                raise ValueError("_field_names must be defined in subclass")
        if record_id:
            self.load()

    def _get_db_field_name(self, name: str) -> str:
        """
        Get the database field name for a property name.

        Args:
            name: Property name or database field name

        Returns:
            Database field name
        """
        # If field_map exists and name is in it, return the mapped field name
        if self._field_map and name in self._field_map:
            return self._field_map[name]
        # Otherwise return the name as-is (it's already a DB field name)
        return name

    def _get_property_name(self, db_field_name: str) -> str:
        """
        Get the property name for a database field name.

        Args:
            db_field_name: Database field name

        Returns:
            Property name (or db_field_name if no mapping exists)
        """
        # If reverse map exists and field is in it, return the property name
        if self._reverse_field_map and db_field_name in self._reverse_field_map:
            return self._reverse_field_map[db_field_name]
        # Otherwise return the db field name as-is
        return db_field_name

    def _get_cache_key(self, record_id: str) -> str:
        """Generate a cache key for a record."""
        return f"{self._file_name}:{record_id}"

    def _get_from_cache(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Get record data from cache if available."""
        if not self._use_cache:
            return None
        cache_key = self._get_cache_key(record_id)
        cached_data = self._record_cache.get(cache_key)
        if cached_data:
            logger.debug(f"Cache hit for record {record_id}")
        return cached_data

    def _put_in_cache(self, record_id: str, data: Dict[str, Any]) -> None:
        """Store record data in cache."""
        if not self._use_cache:
            return

        cache_key = self._get_cache_key(record_id)

        # Simple LRU: if cache is full, remove oldest entry
        if len(self._record_cache) >= self._cache_max_size:
            # Remove first (oldest) entry
            first_key = next(iter(self._record_cache))
            del self._record_cache[first_key]
            logger.debug(f"Cache full, evicted {first_key}")

        self._record_cache[cache_key] = data.copy()
        logger.debug(f"Cached record {record_id}")

    def _invalidate_cache(self, record_id: str) -> None:
        """Remove record from cache."""
        cache_key = self._get_cache_key(record_id)
        if cache_key in self._record_cache:
            del self._record_cache[cache_key]
            logger.debug(f"Invalidated cache for record {record_id}")

    @classmethod
    def clear_cache(cls) -> None:
        """Clear all cached records for this model."""
        cls._record_cache.clear()
        logger.info(f"Cleared cache for {cls._file_name}")

    def load(self) -> None:
        """
        Load the record data from the database.

        Raises:
            ValueError: If record_id is not set
            UOError: If the record cannot be read
        """
        if not self.record_id:
            raise ValueError("record_id must be set before loading")

        # Check cache first
        cached_data = self._get_from_cache(self.record_id)
        if cached_data is not None:
            self.data = cached_data.copy()
            self._is_loaded = True
            return

        logger.debug(f"Loading record {self.record_id} from {self._file_name}")

        with uopy.File(self._file_name, session=self.session) as file_obj:
            # read_named_fields returns: (resp_codes, status_codes, id_list, record_list)
            resp_codes, status_codes, id_list, record_list = file_obj.read_named_fields(
                [self.record_id],
                self._field_names
            )

            # Check if read was successful
            if resp_codes[0] != 0:
                raise uopy.UOError(code=resp_codes[0], message=f"Failed to read record {self.record_id}")

            # Extract the record data
            if record_list and len(record_list) > 0:
                record_data = record_list[0]
                # Map field names to values
                self.data = {
                    field_name: record_data[i] if i < len(record_data) else None
                    for i, field_name in enumerate(self._field_names)
                }
                self._is_loaded = True
                # Store in cache
                self._put_in_cache(self.record_id, self.data)
                logger.debug(f"Loaded record: {self.data}")
            else:
                raise ValueError(f"Record {self.record_id} not found")


    def read(self, record_id: str) -> T:
        """
        Read a record from the database and return a model instance.

        Args:
            session: Active uopy Session object
            record_id: ID of the record to read

        Returns:
            Instance of the model with loaded data
        """
        instance = self.__class__(session=self.session, record_id=record_id)
        return instance


    def select(
        self,
        select_stmt: str, # NAME = "JOHN" AND WITH PHONE = "9497771234"
        limit: Optional[int] = None,
        offset: Optional[int] = 0,
        return_metadata: bool = False
    ):
        """
        Select records from the database based on a SELECT statement.
        Args:
            select_stmt: SELECT statement criteria (e.g., 'NAME = "JOHN"')
            limit: Maximum number of records to return (None = all records)
            offset: Number of records to skip (default: 0)
            return_metadata: If True, return tuple of (instances, metadata) instead of just instances

        Returns:
            List of model instances with loaded data, or tuple of (instances, metadata) if return_metadata=True
            Metadata includes: total_count, returned_count, offset, limit
        """
        logger.debug(f"Executing SELECT statement: {select_stmt} (limit={limit}, offset={offset})")
        instances = []
        metadata = {"total_count": 0, "returned_count": 0, "offset": offset, "limit": limit}

        try:
            cmd_statement = f"SELECT {self._file_name} WITH {select_stmt}"
            file_obj = uopy.Command(cmd_statement, session=self.session)
            output = file_obj.run()
            logger.debug(f"SELECT command output: {output}")

            # Get selected record IDs
            select_list = uopy.List(0, session=self.session).read_list()  # 0 is the default select list

            if select_list:
                total_records = len(select_list)
                metadata["total_count"] = total_records
                logger.debug(f"Found {total_records} records matching SELECT statement")

                # Apply paging to the record IDs
                end_index = offset + limit if limit else len(select_list)
                paged_list = select_list[offset:end_index]
                metadata["returned_count"] = len(paged_list)

                logger.debug(f"Returning records {offset} to {min(end_index, total_records)} (page size: {len(paged_list)})")

                # Call as class method, not instance method
                instances = self.read_many(session=self.session, record_ids=paged_list)
            else:
                logger.debug("No records found matching SELECT statement")
        except Exception as e:
            logger.error(f"Error executing SELECT statement: {e}")
            import traceback
            traceback.print_exc()

        if return_metadata:
            return instances, metadata
        return instances


    def read_many(
        self, # This is an instance method, not a class method
        record_ids: List[str],
        session: uopy.Session = None,
        batch_size: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> List[T]:
        """
        Read multiple records from the database using batch reads for better performance.

        Args:
            session: Active uopy Session object
            record_ids: List of record IDs to read
            batch_size: Number of records to read per batch (None = all at once)
            limit: Maximum number of records to return (None = all records)
            offset: Number of records to skip (default: 0)

        Returns:
            List of model instances with loaded data
        """
        if not record_ids:
            return []

        # Apply paging to the record_ids list before processing
        end_index = offset + limit if limit else len(record_ids)
        paged_ids = record_ids[offset:end_index]

        logger.debug(f"Reading {len(paged_ids)} records from {self._file_name} (batch_size={batch_size}, limit={limit}, offset={offset})")

        instances = []

        # If batch_size is not specified, read all records in one batch
        if batch_size is None:
            batch_size = len(paged_ids)

        # Process records in batches
        for batch_start in range(0, len(paged_ids), batch_size):
            batch_end = min(batch_start + batch_size, len(paged_ids))
            batch_ids = paged_ids[batch_start:batch_end]

            logger.debug(f"Reading batch {batch_start//batch_size + 1}: records {batch_start} to {batch_end}")

            try:
                with uopy.File(self._file_name, session=self.session) as file_obj:
                    # read_named_fields returns: (resp_codes, status_codes, id_list, record_list)
                    resp_codes, status_codes, id_list, record_list = file_obj.read_named_fields(
                        batch_ids,
                        self._field_names
                    )

                    # Process each record in the batch
                    for i, record_id in enumerate(batch_ids):
                        instance = self.__class__.__new__(self.__class__)
                        instance.session = self.session
                        instance.record_id = record_id
                        instance.data = {}
                        instance._reverse_field_map = {v: k for k, v in instance._field_map.items()} if instance._field_map else {}

                        if resp_codes[i] == 0 and i < len(record_list):
                            record_data = record_list[i]
                            instance.data = {
                                field_name: record_data[j] if j < len(record_data) else None
                                for j, field_name in enumerate(self._field_names)
                            }
                            instance._is_loaded = True
                            instances.append(instance)
                        else:
                            logger.warning(f"Failed to read record {record_id}: error code {resp_codes[i]}")
                            # Still add the instance but mark it as not loaded
                            instance._is_loaded = False
                            instances.append(instance)

            except Exception as e:
                logger.error(f"Error reading batch {batch_start//batch_size + 1}: {e}")
                # Create empty instances for failed batch
                for record_id in batch_ids:
                    instance = self.__class__.__new__(self.__class__)
                    instance.session = self.session
                    instance.record_id = record_id
                    instance.data = {}
                    instance._is_loaded = False
                    instance._reverse_field_map = {v: k for k, v in instance._field_map.items()} if instance._field_map else {}
                    instances.append(instance)

        logger.debug(f"Successfully loaded {sum(1 for i in instances if i._is_loaded)} out of {len(paged_ids)} records")
        return instances

    def create(self, record_id: str) -> bool:
        """
        Create a new record in the database.

        Args:
            record_id: ID for the new record

        Returns:
            True if successful

        Raises:
            UOError: If the write fails
        """
        self.record_id = record_id
        logger.debug(f"Creating record {record_id} in {self._file_name}")

        # Prepare field data list in the same order as field_names
        field_data_list = [
            [self.data.get(field_name, "") for field_name in self._field_names]
        ]

        with uopy.File(self._file_name, session=self.session) as file_obj:
            resp_codes, status_codes, id_list, data_list = file_obj.write_named_fields(
                [record_id],
                self._field_names,
                field_data_list
            )

            if int(resp_codes[0]) != 0:
                raise uopy.UOError(code=resp_codes[0], message=f"Failed to create record {record_id}")

            self._is_loaded = True
            # Cache the newly created record
            self._put_in_cache(record_id, self.data)
            logger.info(f"Created record {record_id}")
            return True

    def update(self) -> bool:
        """
        Update the existing record in the database.

        Returns:
            True if successful

        Raises:
            ValueError: If record_id is not set
            UOError: If the write fails
        """
        if not self.record_id:
            raise ValueError("record_id must be set before updating")

        logger.debug(f"Updating record {self.record_id} in {self._file_name}")

        # Prepare field data list in the same order as field_names
        field_data_list = [
            [self.data.get(field_name, "") for field_name in self._field_names]
        ]

        with uopy.File(self._file_name, session=self.session) as file_obj:
            resp_codes, status_codes, id_list, data_list = file_obj.write_named_fields(
                [self.record_id],
                self._field_names,
                field_data_list
            )

            if int(resp_codes[0]) != 0:
                raise uopy.UOError(code=resp_codes[0], message=f"Failed to update record {self.record_id}")

            # Update cache with new data
            self._put_in_cache(self.record_id, self.data)
            logger.info(f"Updated record {self.record_id}")
            return True

    def delete(self) -> bool:
        """
        Delete the record from the database.

        Returns:
            True if successful

        Raises:
            ValueError: If record_id is not set
            UOError: If the delete fails
        """
        if not self.record_id:
            raise ValueError("record_id must be set before deleting")

        logger.debug(f"Deleting record {self.record_id} from {self._file_name}")

        with uopy.File(self._file_name, session=self.session) as file_obj:
            file_obj.delete(self.record_id)

        # Invalidate cache for deleted record
        self._invalidate_cache(self.record_id)
        logger.info(f"Deleted record {self.record_id}")
        self._is_loaded = False
        self.data = {}
        return True

    def save(self) -> bool:
        """
        Save the record (create if new, update if exists).

        Returns:
            True if successful
        """
        if self._is_loaded:
            return self.update()
        else:
            if not self.record_id:
                raise ValueError("record_id must be set before saving")
            return self.create(self.record_id)

    def get(self, field_name: str, default: Any = None) -> Any:
        """
        Get a field value from the record data.
        Supports both property names and database field names.

        Args:
            field_name: Property name or database field name
            default: Default value if field not found

        Returns:
            Field value or default
        """
        db_field_name = self._get_db_field_name(field_name)
        return self.data.get(db_field_name, default)

    def set(self, field_name: str, value: Any) -> None:
        """
        Set a field value in the record data.
        Supports both property names and database field names.

        Args:
            field_name: Property name or database field name
            value: Value to set
        """
        db_field_name = self._get_db_field_name(field_name)
        if db_field_name not in self._field_names:
            logger.warning(f"Field {db_field_name} not in defined field list")
        self.data[db_field_name] = value

    def to_dict(self, use_property_names: bool = False) -> Dict[str, Any]:
        """
        Convert the model to a dictionary.

        Args:
            use_property_names: If True, use property names instead of DB field names

        Returns:
            Dictionary with record_id and data
        """
        data = self.data
        if use_property_names and self._field_map:
            # Convert DB field names to property names
            data = {
                self._get_property_name(k): v
                for k, v in self.data.items()
            }

        return {
            "record_id": self.record_id,
            "data": data,
            "is_loaded": self._is_loaded
        }

    def __getattr__(self, name: str) -> Any:
        """
        Allow direct property access for mapped fields.

        Args:
            name: Property name

        Returns:
            Field value

        Raises:
            AttributeError: If property doesn't exist
        """
        # Avoid infinite recursion - check if we're accessing a real attribute
        if name.startswith('_') or name in ('data', 'session', 'record_id'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        # Check if this is a mapped property
        if self._field_map and name in self._field_map:
            db_field_name = self._field_map[name]
            return self.data.get(db_field_name)

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Allow direct property assignment for mapped fields.

        Args:
            name: Property name
            value: Value to set
        """
        # Handle internal attributes normally
        if name.startswith('_') or name in ('data', 'session', 'record_id'):
            super().__setattr__(name, value)
            return

        # Check if this is a mapped property
        if hasattr(self, '_field_map') and self._field_map and name in self._field_map:
            db_field_name = self._field_map[name]
            if not hasattr(self, 'data'):
                super().__setattr__(name, value)
            else:
                self.data[db_field_name] = value
            return

        # For non-mapped attributes, use normal attribute setting
        super().__setattr__(name, value)

    def __repr__(self) -> str:
        """String representation of the model."""
        return f"<{self.__class__.__name__} record_id={self.record_id} loaded={self._is_loaded}>"

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"{self.__class__.__name__}({self.record_id}): {self.data}"
