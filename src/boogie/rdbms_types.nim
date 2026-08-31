import std/[tables, json]

type
  DataType* = enum
    dtNull, dtInt, dtFloat, dtBool, dtText, dtJson

  ColumnDef* = object
    name*: string
    kind*: DataType
    nullable*: bool
    defaultValue*: string

  Value* = object
    case kind*: DataType
    of dtInt:
      intVal*: int64
    of dtFloat:
      floatVal*: float64
    of dtBool:
      boolVal*: bool
    of dtText:
      strVal*: string
    of dtJson:
      jsonVal*: string
    of dtNull:
      discard

  RowData* = OrderedTable[string, Value]

  PrimaryKeyMode* = enum
    pkmManual, pkmSerial

  ForeignKeyAction* = enum
    fkaRestrict

  ForeignKeyDef* = object
    name*: string
    column*: string
    refTable*: string
    refColumn*: string
    onDelete*: ForeignKeyAction

proc newNullValue*(): Value = Value(kind: dtNull)
proc newIntValue*(v: int64): Value = Value(kind: dtInt, intVal: v)
proc newFloatValue*(v: float64): Value = Value(kind: dtFloat, floatVal: v)
proc newBoolValue*(v: bool): Value = Value(kind: dtBool, boolVal: v)
proc newTextValue*(v: string): Value = Value(kind: dtText, strVal: v)
proc newJSONValue*(v: JsonNode): Value = Value(kind: dtJson, jsonVal: $v)

proc `$`*(v: Value): string =
  case v.kind
  of dtNull: "null"
  of dtInt: $v.intVal
  of dtFloat: $v.floatVal
  of dtBool: $v.boolVal
  of dtText: v.strVal
  of dtJson: v.jsonVal

proc `==`*(a, b: Value): bool =
  if a.kind != b.kind: return false
  case a.kind
  of dtNull: true
  of dtInt: a.intVal == b.intVal
  of dtFloat: a.floatVal == b.floatVal
  of dtBool: a.boolVal == b.boolVal
  of dtText: a.strVal == b.strVal
  of dtJson: a.jsonVal == b.jsonVal
