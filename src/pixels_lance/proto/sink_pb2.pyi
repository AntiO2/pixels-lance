from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TransactionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BEGIN: _ClassVar[TransactionStatus]
    END: _ClassVar[TransactionStatus]

class OperationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INSERT: _ClassVar[OperationType]
    UPDATE: _ClassVar[OperationType]
    DELETE: _ClassVar[OperationType]
    SNAPSHOT: _ClassVar[OperationType]
BEGIN: TransactionStatus
END: TransactionStatus
INSERT: OperationType
UPDATE: OperationType
DELETE: OperationType
SNAPSHOT: OperationType

class PollRequest(_message.Message):
    __slots__ = ("schema_name", "table_name", "buckets")
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKETS_FIELD_NUMBER: _ClassVar[int]
    schema_name: str
    table_name: str
    buckets: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, schema_name: _Optional[str] = ..., table_name: _Optional[str] = ..., buckets: _Optional[_Iterable[int]] = ...) -> None: ...

class PollResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[RowRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[RowRecord, _Mapping]]] = ...) -> None: ...

class DataCollection(_message.Message):
    __slots__ = ("data_collection", "event_count")
    DATA_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    EVENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    data_collection: str
    event_count: int
    def __init__(self, data_collection: _Optional[str] = ..., event_count: _Optional[int] = ...) -> None: ...

class TransactionMetadata(_message.Message):
    __slots__ = ("status", "id", "event_count", "data_collections", "timestamp")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    DATA_COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    status: TransactionStatus
    id: str
    event_count: int
    data_collections: _containers.RepeatedCompositeFieldContainer[DataCollection]
    timestamp: int
    def __init__(self, status: _Optional[_Union[TransactionStatus, str]] = ..., id: _Optional[str] = ..., event_count: _Optional[int] = ..., data_collections: _Optional[_Iterable[_Union[DataCollection, _Mapping]]] = ..., timestamp: _Optional[int] = ...) -> None: ...

class ColumnValue(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class RowValue(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[ColumnValue]
    def __init__(self, values: _Optional[_Iterable[_Union[ColumnValue, _Mapping]]] = ...) -> None: ...

class RowRecord(_message.Message):
    __slots__ = ("before", "after", "source", "transaction", "op")
    BEFORE_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    before: RowValue
    after: RowValue
    source: SourceInfo
    transaction: TransactionInfo
    op: OperationType
    def __init__(self, before: _Optional[_Union[RowValue, _Mapping]] = ..., after: _Optional[_Union[RowValue, _Mapping]] = ..., source: _Optional[_Union[SourceInfo, _Mapping]] = ..., transaction: _Optional[_Union[TransactionInfo, _Mapping]] = ..., op: _Optional[_Union[OperationType, str]] = ...) -> None: ...

class SourceInfo(_message.Message):
    __slots__ = ("db", "schema", "table")
    DB_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    db: str
    schema: str
    table: str
    def __init__(self, db: _Optional[str] = ..., schema: _Optional[str] = ..., table: _Optional[str] = ...) -> None: ...

class TransactionInfo(_message.Message):
    __slots__ = ("id", "total_order", "data_collection_order")
    ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ORDER_FIELD_NUMBER: _ClassVar[int]
    DATA_COLLECTION_ORDER_FIELD_NUMBER: _ClassVar[int]
    id: str
    total_order: int
    data_collection_order: int
    def __init__(self, id: _Optional[str] = ..., total_order: _Optional[int] = ..., data_collection_order: _Optional[int] = ...) -> None: ...
