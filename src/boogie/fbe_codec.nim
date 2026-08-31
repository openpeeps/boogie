# FBE codec for Boogie stores
#
# Single place for all binary serialization that previously used `flatty`.
# Uses `openparser/fbe` low-level Buffer API (versioned structs / writeVector / writeMap).
# `flatty` is kept for migration only (decoding old snapshots / WAL payloads).

import std/[tables, os]
import pkg/flatty
import pkg/openparser/fbe
import ./rdbms_types

# ---------------------------------------------------------------------------
# Generic buffer <-> string for file I/O (binary-safe)
# ---------------------------------------------------------------------------

proc bufToString*(b: Buffer): string =
  if b.data.len == 0: return ""
  result = newString(b.data.len)
  if b.data.len > 0:
    copyMem(addr result[0], addr b.data[0], b.data.len)

proc stringToBuf*(s: string): Buffer =
  result = initBuffer(max(256, s.len + 16))
  result.data.setLen(0)
  if s.len > 0:
    let p = result.appendReserve(s.len)
    copyMem(addr result.data[p], unsafeAddr s[0], s.len)
  result.pos = 0
  result.structStack = @[]

# ---------------------------------------------------------------------------
# Helpers to avoid `writeField` closure (which is not GC-safe)
# ---------------------------------------------------------------------------

proc beginField(b: var Buffer, fid: uint16): int =
  # increment fieldCount and write header placeholder
  b.structStack[^1].fieldCount += 1'u32
  b.writeUint16LE(fid)
  result = b.data.len
  b.writeUint32LE(0'u32)

proc endField(b: var Buffer, sizePos: int) =
  let sz = uint32(b.data.len - (sizePos + 4))
  b.patchUint32At(sizePos, sz)

proc writeFieldUint32(b: var Buffer, fid: uint16, v: uint32) =
  let p = b.beginField(fid)
  b.writeUint32LE(v)
  b.endField(p)

proc writeFieldUint64(b: var Buffer, fid: uint16, v: uint64) =
  let p = b.beginField(fid)
  b.writeUint64LE(v)
  b.endField(p)

proc writeFieldInt32(b: var Buffer, fid: uint16, v: int32) =
  let p = b.beginField(fid)
  b.writeInt32LE(v)
  b.endField(p)

proc writeFieldInt64(b: var Buffer, fid: uint16, v: int64) =
  let p = b.beginField(fid)
  b.writeInt64LE(v)
  b.endField(p)

proc writeFieldBool(b: var Buffer, fid: uint16, v: bool) =
  let p = b.beginField(fid)
  b.writeBool(v)
  b.endField(p)

proc writeFieldString(b: var Buffer, fid: uint16, s: string) =
  let p = b.beginField(fid)
  b.writeString(s)
  b.endField(p)

proc writeFieldByte(b: var Buffer, fid: uint16, v: uint8) =
  let p = b.beginField(fid)
  b.writeByte(v)
  b.endField(p)

proc writeFieldFloat32(b: var Buffer, fid: uint16, v: float32) =
  let p = b.beginField(fid)
  b.writeFloat32LE(v)
  b.endField(p)

proc writeFieldFloat64(b: var Buffer, fid: uint16, v: float64) =
  let p = b.beginField(fid)
  b.writeFloat64LE(v)
  b.endField(p)

# ---------------------------------------------------------------------------
# RDBMS Value / RowData (variant + OrderedTable) — also WAL payload
# ---------------------------------------------------------------------------

proc writeValue(b: var Buffer, v: Value) =
  beginInnerStruct(b, 1)
  b.writeFieldByte(1, uint8(ord(v.kind)))
  case v.kind
  of dtInt:
    b.writeFieldInt64(2, v.intVal)
  of dtFloat:
    b.writeFieldFloat64(3, v.floatVal)
  of dtBool:
    b.writeFieldBool(4, v.boolVal)
  of dtText:
    b.writeFieldString(5, v.strVal)
  of dtJson:
    b.writeFieldString(6, v.jsonVal)
  of dtNull:
    discard
  endInnerStruct(b)

proc readValue(b: var Buffer): Value =
  var kindByte: uint8 = 0
  var intVal: int64 = 0
  var floatVal: float64 = 0
  var boolVal = false
  var strVal = ""
  var jsonVal = ""
  discard beginReadInnerStruct(b)
  while true:
    var fid: uint16
    var fsz: int
    if not readFieldHeader(b, fid, fsz): break
    case fid
    of 1:
      kindByte = readFieldValue[uint8](b, fsz, proc(bb: var Buffer): uint8 = bb.readByte())
    of 2:
      intVal = readFieldValue[int64](b, fsz, proc(bb: var Buffer): int64 = bb.readInt64LE())
    of 3:
      floatVal = readFieldValue[float64](b, fsz, proc(bb: var Buffer): float64 = bb.readFloat64LE())
    of 4:
      boolVal = readFieldValue[bool](b, fsz, proc(bb: var Buffer): bool = bb.readBool())
    of 5:
      strVal = readFieldValue[string](b, fsz, proc(bb: var Buffer): string = bb.readString())
    of 6:
      jsonVal = readFieldValue[string](b, fsz, proc(bb: var Buffer): string = bb.readString())
    else:
      # skip unknown field payload (already consumed via readFieldHeader size, need to skip)
      if fsz > 0:
        b.skipBytes(fsz)
      else:
        discard
      # compensate because readFieldValue would have advanced; we already are at field end
      # Actually we didn't call readFieldValue, so manually advance if not consumed
      # But we already have field header consumed and need to skip payload we didn't read above for known fids we did read via readFieldValue
      # For unknown, payload not yet consumed because we didn't read - but readFieldValue not called, so we are at payload start; skip it
      # The above skip handles it
      # For known fids we consumed via readFieldValue which already advanced to field end, so no skip needed
      # This branch is only for unknown, so skip is correct
    # Note: for known fids we consumed via readFieldValue which internally ensures pos == payloadEnd
  endReadInnerStruct(b)
  let k = DataType(kindByte)
  case k
  of dtInt: result = newIntValue(intVal)
  of dtFloat: result = newFloatValue(floatVal)
  of dtBool: result = newBoolValue(boolVal)
  of dtText: result = newTextValue(strVal)
  of dtJson:
    # jsonVal is raw JSON text; construct via Value directly
    result = Value(kind: dtJson, jsonVal: jsonVal)
  of dtNull: result = newNullValue()

proc writeRowData(b: var Buffer, d: RowData) =
  # Encode OrderedTable as vector of (key, Value) pairs preserving order.
  # Stored as a field value (called from parent writeField), we write inner struct containing one vector field.
  # To keep nesting minimal, caller wraps this in its own field; here we just write the vector encoding directly.
  # However `writeValue` uses inner struct, so this is used as payload inside a parent field.
  # We encode as: uint32 count + repeated (string key, Value inner struct)
  var items = newSeq[tuple[key:string, val:Value]](d.len)
  var i = 0
  for k, v in d.pairs:
    items[i] = (key: k, val: v)
    inc i
  b.writeUint32LE(uint32(items.len))
  for it in items:
    b.writeString(it.key)
    writeValue(b, it.val)

proc readRowData(b: var Buffer): RowData =
  result = initOrderedTable[string, Value]()
  let n = int(b.readUint32LE())
  for _ in 0..<n:
    let k = b.readString()
    let v = readValue(b)
    result[k] = v

# Public WAL payload helpers (RDBMS)

proc encodeRowPayload*(data: RowData): string =
  var buf = initBuffer(256 + data.len * 32)
  buf.writeRowData(data)
  bufToString(buf)

proc decodeRowPayload*(payload: string): RowData =
  if payload.len == 0:
    return initOrderedTable[string, Value]()
  var buf = stringToBuf(payload)
  result = readRowData(buf)

proc isFbeRowPayload*(payload: string): bool =
  if payload.len < 4: return false
  try:
    var buf = stringToBuf(payload)
    let rd = readRowData(buf)
    if buf.pos != buf.data.len: return false
    # re-encode must match exactly to be FBE
    result = encodeRowPayload(rd) == payload
  except CatchableError:
    result = false

# ---------------------------------------------------------------------------
# Helpers for ColumnDef / ForeignKeyDef / RDBMS snapshot
# ---------------------------------------------------------------------------

proc writeColumnDef(b: var Buffer, c: ColumnDef) =
  beginInnerStruct(b, 1)
  b.writeFieldString(1, c.name)
  b.writeFieldByte(2, uint8(ord(c.kind)))
  b.writeFieldBool(3, c.nullable)
  b.writeFieldString(4, c.defaultValue)
  endInnerStruct(b)

proc readColumnDef(b: var Buffer): ColumnDef =
  var name = ""
  var kindByte: uint8 = 0
  var nullable = false
  var defaultValue = ""
  discard beginReadInnerStruct(b)
  while true:
    var fid: uint16
    var fsz: int
    if not readFieldHeader(b, fid, fsz): break
    case fid
    of 1: name = readFieldValue[string](b, fsz, proc(bb: var Buffer): string = bb.readString())
    of 2: kindByte = readFieldValue[uint8](b, fsz, proc(bb: var Buffer): uint8 = bb.readByte())
    of 3: nullable = readFieldValue[bool](b, fsz, proc(bb: var Buffer): bool = bb.readBool())
    of 4: defaultValue = readFieldValue[string](b, fsz, proc(bb: var Buffer): string = bb.readString())
    else: b.skipBytes(fsz)
  endReadInnerStruct(b)
  result = ColumnDef(name: name, kind: DataType(kindByte), nullable: nullable, defaultValue: defaultValue)

proc writeForeignKeyDef(b: var Buffer, fk: ForeignKeyDef) =
  beginInnerStruct(b, 1)
  b.writeFieldString(1, fk.name)
  b.writeFieldString(2, fk.column)
  b.writeFieldString(3, fk.refTable)
  b.writeFieldString(4, fk.refColumn)
  b.writeFieldByte(5, uint8(ord(fk.onDelete)))
  endInnerStruct(b)

proc readForeignKeyDef(b: var Buffer): ForeignKeyDef =
  var name, column, refTable, refColumn: string
  var onDeleteByte: uint8 = 0
  discard beginReadInnerStruct(b)
  while true:
    var fid: uint16
    var fsz: int
    if not readFieldHeader(b, fid, fsz): break
    case fid
    of 1: name = readFieldValue[string](b, fsz, proc(bb: var Buffer): string = bb.readString())
    of 2: column = readFieldValue[string](b, fsz, proc(bb: var Buffer): string = bb.readString())
    of 3: refTable = readFieldValue[string](b, fsz, proc(bb: var Buffer): string = bb.readString())
    of 4: refColumn = readFieldValue[string](b, fsz, proc(bb: var Buffer): string = bb.readString())
    of 5: onDeleteByte = readFieldValue[uint8](b, fsz, proc(bb: var Buffer): uint8 = bb.readByte())
    else: b.skipBytes(fsz)
  endReadInnerStruct(b)
  result = ForeignKeyDef(name: name, column: column, refTable: refTable, refColumn: refColumn, onDelete: ForeignKeyAction(onDeleteByte))

# ---------------------------------------------------------------------------
# KV snapshot (version, checkpointLsn, entries)
# ---------------------------------------------------------------------------

type KvSnapshotOnDisk* = tuple
  version: uint32
  checkpointLsn: uint64
  entries: seq[(string, string)]

proc encodeKvSnapshot*(b: var Buffer, snap: KvSnapshotOnDisk) =
  beginRootStruct(b, 1)
  b.writeFieldUint32(1, snap.version)
  b.writeFieldUint64(2, snap.checkpointLsn)
  let p3 = b.beginField(3)
  b.writeUint32LE(uint32(snap.entries.len))
  for (k, v) in snap.entries:
    b.writeString(k)
    b.writeString(v)
  b.endField(p3)
  endRootStruct(b)

proc decodeKvSnapshot*(b: var Buffer): KvSnapshotOnDisk =
  var version: uint32 = 0
  var checkpointLsn: uint64 = 0
  var entries: seq[(string, string)] = @[]
  discard beginReadRootStruct(b)
  while true:
    var fid: uint16
    var fsz: int
    if not readFieldHeader(b, fid, fsz): break
    case fid
    of 1: version = readFieldValue[uint32](b, fsz, proc(bb: var Buffer): uint32 = bb.readUint32LE())
    of 2: checkpointLsn = readFieldValue[uint64](b, fsz, proc(bb: var Buffer): uint64 = bb.readUint64LE())
    of 3:
      let start = b.pos
      let n = int(b.readUint32LE())
      entries = newSeq[(string, string)](n)
      for i in 0..<n:
        let k = b.readString()
        let v = b.readString()
        entries[i] = (k, v)
      if b.pos < start + fsz: b.pos = start + fsz
    else: b.skipBytes(fsz)
  endReadRootStruct(b)
  result = (version: version, checkpointLsn: checkpointLsn, entries: entries)

proc encodeKvSnapshotToString*(snap: KvSnapshotOnDisk): string =
  var buf = initBuffer(256 + snap.entries.len * 64)
  encodeKvSnapshot(buf, snap)
  bufToString(buf)

proc decodeKvSnapshotFromString*(s: string): KvSnapshotOnDisk =
  var buf = stringToBuf(s)
  decodeKvSnapshot(buf)

# ---------------------------------------------------------------------------
# DocStore snapshot (version, checkpointLsn, docs: seq[(string,string)])
# ---------------------------------------------------------------------------

type DocSnapshotOnDisk* = tuple
  version: uint32
  checkpointLsn: uint64
  docs: seq[(string, string)]

proc encodeDocSnapshot*(b: var Buffer, snap: DocSnapshotOnDisk) =
  beginRootStruct(b, 1)
  b.writeFieldUint32(1, snap.version)
  b.writeFieldUint64(2, snap.checkpointLsn)
  let p3 = b.beginField(3)
  b.writeUint32LE(uint32(snap.docs.len))
  for (k, v) in snap.docs:
    b.writeString(k)
    b.writeString(v)
  b.endField(p3)
  endRootStruct(b)

proc decodeDocSnapshot*(b: var Buffer): DocSnapshotOnDisk =
  var version: uint32 = 0
  var checkpointLsn: uint64 = 0
  var docs: seq[(string, string)] = @[]
  discard beginReadRootStruct(b)
  while true:
    var fid: uint16
    var fsz: int
    if not readFieldHeader(b, fid, fsz): break
    case fid
    of 1: version = readFieldValue[uint32](b, fsz, proc(bb: var Buffer): uint32 = bb.readUint32LE())
    of 2: checkpointLsn = readFieldValue[uint64](b, fsz, proc(bb: var Buffer): uint64 = bb.readUint64LE())
    of 3:
      let start = b.pos
      let n = int(b.readUint32LE())
      docs = newSeq[(string, string)](n)
      for i in 0..<n:
        docs[i] = (b.readString(), b.readString())
      if b.pos < start + fsz: b.pos = start + fsz
    else: b.skipBytes(fsz)
  endReadRootStruct(b)
  result = (version: version, checkpointLsn: checkpointLsn, docs: docs)

proc encodeDocSnapshotToString*(snap: DocSnapshotOnDisk): string =
  var buf = initBuffer(256 + snap.docs.len * 64)
  encodeDocSnapshot(buf, snap)
  bufToString(buf)

proc decodeDocSnapshotFromString*(s: string): DocSnapshotOnDisk =
  var buf = stringToBuf(s)
  decodeDocSnapshot(buf)

# ---------------------------------------------------------------------------
# VectorStore snapshot (version, checkpointLsn, collections)
# ---------------------------------------------------------------------------

type VectorSnapshotOnDisk* = tuple
  version: uint32
  checkpointLsn: uint64
  collections: seq[tuple[name: string, dimension: int, rows: seq[(string, seq[float32], string)]]]

proc encodeVectorSnapshot*(b: var Buffer, snap: VectorSnapshotOnDisk) =
  beginRootStruct(b, 1)
  b.writeFieldUint32(1, snap.version)
  b.writeFieldUint64(2, snap.checkpointLsn)
  let p3 = b.beginField(3)
  b.writeUint32LE(uint32(snap.collections.len))
  for i in 0..<snap.collections.len:
    let col = snap.collections[i]
    beginInnerStruct(b, 1)
    b.writeFieldString(1, col.name)
    b.writeFieldInt32(2, int32(col.dimension))
    let pInner = b.beginField(3)
    b.writeUint32LE(uint32(col.rows.len))
    for (pk, vec, part) in col.rows:
      b.writeString(pk)
      b.writeUint32LE(uint32(vec.len))
      for x in vec:
        b.writeFloat32LE(x)
      b.writeString(part)
    b.endField(pInner)
    endInnerStruct(b)
  b.endField(p3)
  endRootStruct(b)

proc decodeVectorSnapshot*(b: var Buffer): VectorSnapshotOnDisk =
  var version: uint32 = 0
  var checkpointLsn: uint64 = 0
  var collections: seq[tuple[name: string, dimension: int, rows: seq[(string, seq[float32], string)]]] = @[]
  discard beginReadRootStruct(b)
  while true:
    var fid: uint16
    var fsz: int
    if not readFieldHeader(b, fid, fsz): break
    case fid
    of 1: version = readFieldValue[uint32](b, fsz, proc(bb: var Buffer): uint32 = bb.readUint32LE())
    of 2: checkpointLsn = readFieldValue[uint64](b, fsz, proc(bb: var Buffer): uint64 = bb.readUint64LE())
    of 3:
      let start = b.pos
      let n = int(b.readUint32LE())
      collections = newSeq[tuple[name: string, dimension: int, rows: seq[(string, seq[float32], string)]]](n)
      for i in 0..<n:
        discard beginReadInnerStruct(b)
        var name = ""
        var dimension = 0
        var rows: seq[(string, seq[float32], string)] = @[]
        while true:
          var fid2: uint16
          var fsz2: int
          if not readFieldHeader(b, fid2, fsz2): break
          case fid2
          of 1: name = readFieldValue[string](b, fsz2, proc(bb: var Buffer): string = bb.readString())
          of 2: dimension = int(readFieldValue[int32](b, fsz2, proc(bb: var Buffer): int32 = bb.readInt32LE()))
          of 3:
            let s3 = b.pos
            let rn = int(b.readUint32LE())
            rows = newSeq[(string, seq[float32], string)](rn)
            for ri in 0..<rn:
              let pk = b.readString()
              let vlen = int(b.readUint32LE())
              var vec = newSeq[float32](vlen)
              for vi in 0..<vlen: vec[vi] = b.readFloat32LE()
              let part = b.readString()
              rows[ri] = (pk, vec, part)
            if b.pos < s3 + fsz2: b.pos = s3 + fsz2
          else: b.skipBytes(fsz2)
        endReadInnerStruct(b)
        collections[i] = (name: name, dimension: dimension, rows: rows)
      if b.pos < start + fsz: b.pos = start + fsz
    else: b.skipBytes(fsz)
  endReadRootStruct(b)
  result = (version: version, checkpointLsn: checkpointLsn, collections: collections)

proc encodeVectorSnapshotToString*(snap: VectorSnapshotOnDisk): string =
  var buf = initBuffer(256 + snap.collections.len * 256)
  encodeVectorSnapshot(buf, snap)
  bufToString(buf)

proc decodeVectorSnapshotFromString*(s: string): VectorSnapshotOnDisk =
  var buf = stringToBuf(s)
  decodeVectorSnapshot(buf)

# ---------------------------------------------------------------------------
# RDBMS snapshot (version, checkpointLsn, tables)
# ---------------------------------------------------------------------------

type RdbmsSnapshotOnDisk* = tuple
  version: uint32
  checkpointLsn: uint64
  tables: seq[tuple[
    name: string,
    primaryKey: string,
    pkType: PrimaryKeyMode,
    pkSequence: uint64,
    columns: seq[ColumnDef],
    foreignKeys: seq[ForeignKeyDef],
    rows: seq[(string, RowData)]
  ]]

proc encodeRdbmsSnapshot*(b: var Buffer, snap: RdbmsSnapshotOnDisk) =
  beginRootStruct(b, 1)
  b.writeFieldUint32(1, snap.version)
  b.writeFieldUint64(2, snap.checkpointLsn)
  let p3 = b.beginField(3)
  b.writeUint32LE(uint32(snap.tables.len))
  for ti in 0..<snap.tables.len:
    let t = snap.tables[ti]
    beginInnerStruct(b, 1)
    b.writeFieldString(1, t.name)
    b.writeFieldString(2, t.primaryKey)
    b.writeFieldByte(3, uint8(ord(t.pkType)))
    b.writeFieldUint64(4, t.pkSequence)
    let p5 = b.beginField(5)
    b.writeUint32LE(uint32(t.columns.len))
    for ci in 0..<t.columns.len: writeColumnDef(b, t.columns[ci])
    b.endField(p5)
    let p6 = b.beginField(6)
    b.writeUint32LE(uint32(t.foreignKeys.len))
    for fki in 0..<t.foreignKeys.len: writeForeignKeyDef(b, t.foreignKeys[fki])
    b.endField(p6)
    let p7 = b.beginField(7)
    b.writeUint32LE(uint32(t.rows.len))
    for (pk, rd) in t.rows:
      b.writeString(pk)
      writeRowData(b, rd)
    b.endField(p7)
    endInnerStruct(b)
  b.endField(p3)
  endRootStruct(b)

proc decodeRdbmsSnapshot*(b: var Buffer): RdbmsSnapshotOnDisk =
  var version: uint32 = 0
  var checkpointLsn: uint64 = 0
  var tables: seq[tuple[name: string, primaryKey: string, pkType: PrimaryKeyMode, pkSequence: uint64, columns: seq[ColumnDef], foreignKeys: seq[ForeignKeyDef], rows: seq[(string, RowData)]]] = @[]
  discard beginReadRootStruct(b)
  while true:
    var fid: uint16
    var fsz: int
    if not readFieldHeader(b, fid, fsz): break
    case fid
    of 1: version = readFieldValue[uint32](b, fsz, proc(bb: var Buffer): uint32 = bb.readUint32LE())
    of 2: checkpointLsn = readFieldValue[uint64](b, fsz, proc(bb: var Buffer): uint64 = bb.readUint64LE())
    of 3:
      let start = b.pos
      let n = int(b.readUint32LE())
      tables = newSeq[tuple[name: string, primaryKey: string, pkType: PrimaryKeyMode, pkSequence: uint64, columns: seq[ColumnDef], foreignKeys: seq[ForeignKeyDef], rows: seq[(string, RowData)]]](n)
      for i in 0..<n:
        discard beginReadInnerStruct(b)
        var name, primaryKey: string
        var pkTypeByte: uint8 = 0
        var pkSequence: uint64 = 0
        var columns: seq[ColumnDef] = @[]
        var foreignKeys: seq[ForeignKeyDef] = @[]
        var rows: seq[(string, RowData)] = @[]
        while true:
          var fid2: uint16
          var fsz2: int
          if not readFieldHeader(b, fid2, fsz2): break
          case fid2
          of 1: name = readFieldValue[string](b, fsz2, proc(bb: var Buffer): string = bb.readString())
          of 2: primaryKey = readFieldValue[string](b, fsz2, proc(bb: var Buffer): string = bb.readString())
          of 3: pkTypeByte = readFieldValue[uint8](b, fsz2, proc(bb: var Buffer): uint8 = bb.readByte())
          of 4: pkSequence = readFieldValue[uint64](b, fsz2, proc(bb: var Buffer): uint64 = bb.readUint64LE())
          of 5:
            let s5 = b.pos
            let cn = int(b.readUint32LE())
            columns = newSeq[ColumnDef](cn)
            for ci in 0..<cn: columns[ci] = readColumnDef(b)
            if b.pos < s5 + fsz2: b.pos = s5 + fsz2
          of 6:
            let s6 = b.pos
            let fkN = int(b.readUint32LE())
            foreignKeys = newSeq[ForeignKeyDef](fkN)
            for fi in 0..<fkN: foreignKeys[fi] = readForeignKeyDef(b)
            if b.pos < s6 + fsz2: b.pos = s6 + fsz2
          of 7:
            let s7 = b.pos
            let rn = int(b.readUint32LE())
            rows = newSeq[(string, RowData)](rn)
            for ri in 0..<rn:
              let pk = b.readString()
              let rd = readRowData(b)
              rows[ri] = (pk, rd)
            if b.pos < s7 + fsz2: b.pos = s7 + fsz2
          else: b.skipBytes(fsz2)
        endReadInnerStruct(b)
        tables[i] = (name: name, primaryKey: primaryKey, pkType: PrimaryKeyMode(pkTypeByte), pkSequence: pkSequence, columns: columns, foreignKeys: foreignKeys, rows: rows)
      if b.pos < start + fsz: b.pos = start + fsz
    else: b.skipBytes(fsz)
  endReadRootStruct(b)
  result = (version: version, checkpointLsn: checkpointLsn, tables: tables)

proc encodeRdbmsSnapshotToString*(snap: RdbmsSnapshotOnDisk): string =
  var buf = initBuffer(4096 + snap.tables.len * 512)
  encodeRdbmsSnapshot(buf, snap)
  bufToString(buf)

proc decodeRdbmsSnapshotFromString*(s: string): RdbmsSnapshotOnDisk =
  var buf = stringToBuf(s)
  decodeRdbmsSnapshot(buf)

# ---------------------------------------------------------------------------
# Migration API — flatty -> FBE
# Keep `flatty` only for reading old blobs; all new writes are FBE.
# ---------------------------------------------------------------------------

proc decodeRowPayloadFlatty*(payload: string): RowData =
  ## Decode a legacy flatty-encoded RowData payload (old WAL).
  fromFlatty(payload, RowData)

proc migrateRowPayloadFlattyToFbe*(payload: string): string =
  ## Convert a flatty RowData payload to FBE. If payload is already FBE or empty, returns as-is.
  if payload.len == 0: return payload
  if isFbeRowPayload(payload):
    return payload
  let rd = decodeRowPayloadFlatty(payload)
  encodeRowPayload(rd)

proc decodeRowPayloadWithFallback*(payload: string): RowData =
  ## WAL replay helper: try FBE first, fall back to flatty.
  if payload.len == 0:
    return initOrderedTable[string, Value]()
  if isFbeRowPayload(payload):
    var buf = stringToBuf(payload)
    result = readRowData(buf)
  else:
    result = fromFlatty(payload, RowData)

proc isFbeKvSnapshot*(blob: string): bool =
  if blob.len < 16: return false
  try:
    var buf = stringToBuf(blob)
    let snap = decodeKvSnapshot(buf)
    if buf.pos != buf.data.len: return false
    result = encodeKvSnapshotToString(snap) == blob
  except CatchableError:
    result = false

# Kv flatty helpers

proc decodeKvSnapshotFlatty*(blob: string): KvSnapshotOnDisk =
  fromFlatty(blob, KvSnapshotOnDisk)

proc migrateKvFlattyBlobToFbe*(blob: string): string =
  encodeKvSnapshotToString(decodeKvSnapshotFlatty(blob))

proc decodeKvSnapshotFromStringWithFallback*(s: string): KvSnapshotOnDisk =
  if isFbeKvSnapshot(s):
    var buf = stringToBuf(s)
    result = decodeKvSnapshot(buf)
  else:
    result = fromFlatty(s, KvSnapshotOnDisk)

proc migrateKvSnapshotFileFlattyToFbe*(dbPath: string): bool =
  ## If `dbPath` exists and contains a flatty blob, rewrite it as FBE. Returns true if migrated.
  if not fileExists(dbPath): return false
  let blob = readFile(dbPath)
  if blob.len == 0: return false
  if isFbeKvSnapshot(blob): return false
  let fbeBlob = migrateKvFlattyBlobToFbe(blob)
  let tmp = dbPath & ".tmp"
  writeFile(tmp, fbeBlob)
  if fileExists(dbPath): removeFile(dbPath)
  moveFile(tmp, dbPath)
  true

# Doc flatty helpers

proc isFbeDocSnapshot*(blob: string): bool =
  if blob.len < 16: return false
  try:
    var buf = stringToBuf(blob)
    let snap = decodeDocSnapshot(buf)
    if buf.pos != buf.data.len: return false
    result = encodeDocSnapshotToString(snap) == blob
  except CatchableError:
    result = false

proc decodeDocSnapshotFlatty*(blob: string): DocSnapshotOnDisk =
  fromFlatty(blob, DocSnapshotOnDisk)

proc migrateDocFlattyBlobToFbe*(blob: string): string =
  encodeDocSnapshotToString(decodeDocSnapshotFlatty(blob))

proc decodeDocSnapshotFromStringWithFallback*(s: string): DocSnapshotOnDisk =
  if isFbeDocSnapshot(s):
    var buf = stringToBuf(s)
    result = decodeDocSnapshot(buf)
  else:
    result = fromFlatty(s, DocSnapshotOnDisk)

proc migrateDocSnapshotFileFlattyToFbe*(dbPath: string): bool =
  if not fileExists(dbPath): return false
  let blob = readFile(dbPath)
  if blob.len == 0: return false
  if isFbeDocSnapshot(blob): return false
  let fbeBlob = migrateDocFlattyBlobToFbe(blob)
  let tmp = dbPath & ".tmp"
  writeFile(tmp, fbeBlob)
  if fileExists(dbPath): removeFile(dbPath)
  moveFile(tmp, dbPath)
  true

# Vector flatty helpers

proc isFbeVectorSnapshot*(blob: string): bool =
  if blob.len < 16: return false
  try:
    var buf = stringToBuf(blob)
    let snap = decodeVectorSnapshot(buf)
    if buf.pos != buf.data.len: return false
    result = encodeVectorSnapshotToString(snap) == blob
  except CatchableError:
    result = false

proc decodeVectorSnapshotFlatty*(blob: string): VectorSnapshotOnDisk =
  fromFlatty(blob, VectorSnapshotOnDisk)

proc migrateVectorFlattyBlobToFbe*(blob: string): string =
  encodeVectorSnapshotToString(decodeVectorSnapshotFlatty(blob))

proc decodeVectorSnapshotFromStringWithFallback*(s: string): VectorSnapshotOnDisk =
  if isFbeVectorSnapshot(s):
    var buf = stringToBuf(s)
    result = decodeVectorSnapshot(buf)
  else:
    result = fromFlatty(s, VectorSnapshotOnDisk)

proc migrateVectorSnapshotFileFlattyToFbe*(dbPath: string): bool =
  if not fileExists(dbPath): return false
  let blob = readFile(dbPath)
  if blob.len == 0: return false
  if isFbeVectorSnapshot(blob): return false
  let fbeBlob = migrateVectorFlattyBlobToFbe(blob)
  let tmp = dbPath & ".tmp"
  writeFile(tmp, fbeBlob)
  if fileExists(dbPath): removeFile(dbPath)
  moveFile(tmp, dbPath)
  true

# Rdbms flatty helpers

proc isFbeRdbmsSnapshot*(blob: string): bool =
  if blob.len < 16: return false
  try:
    var buf = stringToBuf(blob)
    discard decodeRdbmsSnapshot(buf)
    result = buf.pos == buf.data.len
  except CatchableError:
    result = false

proc decodeRdbmsSnapshotFlatty*(blob: string): RdbmsSnapshotOnDisk =
  fromFlatty(blob, RdbmsSnapshotOnDisk)

proc migrateRdbmsFlattyBlobToFbe*(blob: string): string =
  encodeRdbmsSnapshotToString(decodeRdbmsSnapshotFlatty(blob))

proc decodeRdbmsSnapshotFromStringWithFallback*(s: string, allowFlatty = true): RdbmsSnapshotOnDisk =
  if isFbeRdbmsSnapshot(s):
    var buf = stringToBuf(s)
    result = decodeRdbmsSnapshot(buf)
  elif allowFlatty:
    result = fromFlatty(s, RdbmsSnapshotOnDisk)
  else:
    # No flatty fallback by default to avoid OOM on large flatty blobs.
    # Caller can opt-in via allowFlatty=true or explicit migrate helper.
    var buf = stringToBuf(s)
    result = decodeRdbmsSnapshot(buf)

proc migrateRdbmsSnapshotFileFlattyToFbe*(dbPath: string): bool =
  if not fileExists(dbPath): return false
  let blob = readFile(dbPath)
  if blob.len == 0: return false
  if isFbeRdbmsSnapshot(blob): return false
  let fbeBlob = migrateRdbmsFlattyBlobToFbe(blob)
  let tmp = dbPath & ".tmp"
  writeFile(tmp, fbeBlob)
  if fileExists(dbPath): removeFile(dbPath)
  moveFile(tmp, dbPath)
  true

proc migrateStoreSnapshotsFlattyToFbe*(basePath: string): tuple[kv, doc, vector, rdbms: bool] =
  ## Convenience: migrate any known snapshot files next to `basePath` (checks .db/.ddb/.vdb).
  result.kv = migrateKvSnapshotFileFlattyToFbe(basePath.changeFileExt(".db"))
  result.doc = migrateDocSnapshotFileFlattyToFbe(basePath.changeFileExt(".ddb"))
  result.vector = migrateVectorSnapshotFileFlattyToFbe(basePath.changeFileExt(".vdb"))
  result.rdbms = migrateRdbmsSnapshotFileFlattyToFbe(basePath.changeFileExt(".db"))
