from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ProductRequest(_message.Message):
    __slots__ = ("product_id",)
    PRODUCT_ID_FIELD_NUMBER: _ClassVar[int]
    product_id: str
    def __init__(self, product_id: _Optional[str] = ...) -> None: ...

class ProductResponse(_message.Message):
    __slots__ = ("product_id", "name", "price", "stock_quantity")
    PRODUCT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    STOCK_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    product_id: str
    name: str
    price: float
    stock_quantity: int
    def __init__(self, product_id: _Optional[str] = ..., name: _Optional[str] = ..., price: _Optional[float] = ..., stock_quantity: _Optional[int] = ...) -> None: ...
