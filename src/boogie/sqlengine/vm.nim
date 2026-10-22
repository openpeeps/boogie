# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

## Minimal SQL VM for the Boogie SQL engine.
##
## This module replaces the vancode dependency for SQL execution. It owns a
## small stack-based bytecode interpreter tailored to what the SQL lowerer
## actually emits: constants, globals (placeholders), locals (scan handles
## and aggregate slots), jumps, direct foreign calls for table access, and
## NULL-aware SQL comparison/arithmetic/aggregate opcodes.
##
## The raw bytecode format intentionally mirrors the previous chunk layout
## (opcodes followed by uint8/uint16/int64/float64 operands, string table
## for literals and global names, forward-jump hole patching), so the
## lowering code in `boogie/sqlengine` stays almost unchanged.

import std/[tables, hashes, json, strutils]

const
  tyNil* = 0
  tyBool* = 1
  tyInt* = 2
  tyFloat* = 3
  tyString* = 4
  tyJsonStorage* = 11

type
  TypeId* = range[0..32766]

  Value* {.acyclic.} = ref object
    ## Runtime value for the SQL VM. SQL NULL is a plain nil `Value` ref,
    ## which never appears as a legal stack value otherwise.
    case typeId*: TypeId
    of tyBool:
      boolVal*: bool
    of tyInt:
      intVal*: int64
    of tyFloat:
      floatVal*: float64
    of tyString:
      stringVal*: ref string
    of tyJsonStorage:
      jsonVal*: JsonNode
    else:
      discard

  Stack* = seq[Value]

  StackView* = ptr UncheckedArray[Value]
    ## Unsafe view into a stack, used for foreign-proc arguments.

  ForeignProc* = proc(args: StackView, argc: int): Value
    ## Foreign (Nim) procedure called directly from bytecode.

proc initValue*(v: bool): Value =
  result = Value(typeId: tyBool, boolVal: v)

proc initValue*(v: int64): Value =
  result = Value(typeId: tyInt, intVal: v)

proc initValue*(v: float64): Value =
  result = Value(typeId: tyFloat, floatVal: v)

proc initValue*(v: string): Value =
  result = Value(typeId: tyString)
  new(result.stringVal)
  result.stringVal[] = v

proc initValue*(v: JsonNode): Value =
  result = Value(typeId: tyJsonStorage)
  result.jsonVal = v

proc isSqlNull*(v: Value): bool =
  ## True when the VM value represents SQL NULL.
  v == nil

type
  Opcode* = enum
    opcPushI = "pushI"
    opcPushF = "pushF"
    opcPushS = "pushS"
    opcPushTrue = "pushTrue"
    opcPushFalse = "pushFalse"
    opcPushG = "pushG"
    opcPopG = "popG"
    opcPushL = "pushL"
    opcPopL = "popL"
    opcIncL = "incL"
    opcDecL = "decL"
    opcEqI = "eqI"
    opcInvB = "invB"
    opcJumpFwd = "jumpFwd"
    opcJumpFwdT = "jumpFwdT"
    opcJumpFwdF = "jumpFwdF"
    opcJumpBack = "jumpBack"
    opcCallD = "callD"
    opcDiscard = "discard"
    opcHalt = "halt"
    opcSqlCmp = "sqlCmp"
    opcSqlIsNull = "sqlIsNull"
    opcSqlArith = "sqlArith"
    opcSqlAgg = "sqlAgg"

const SqlCmpEq* = 0'u8
  ## `opcSqlCmp` mode constants
const SqlCmpNe* = 1'u8
const SqlCmpLt* = 2'u8
const SqlCmpLe* = 3'u8
const SqlCmpGt* = 4'u8
const SqlCmpGe* = 5'u8

const SqlArithAdd* = 0'u8
  ## `opcSqlArith` mode constants
const SqlArithSub* = 1'u8
const SqlArithMul* = 2'u8
const SqlArithDiv* = 3'u8

const SqlAggCountStar* = 0'u8
  ## `opcSqlAgg` mode constants
const SqlAggSum* = 1'u8
const SqlAggMin* = 2'u8
const SqlAggMax* = 3'u8
const SqlAggCount* = 4'u8
const SqlAggAvg* = 5'u8

type
  ProcKind* = enum
    pkNative
    pkForeign

  Proc* {.acyclic.} = ref object
    ## A runtime procedure. The SQL engine only registers foreign procs;
    ## `pkNative` is rejected at call time with a clear error.
    name*: string
    case kind*: ProcKind
    of pkNative:
      chunk*: Chunk
    of pkForeign:
      foreign*: ForeignProc
    paramCount*: int
    hasResult*: bool

  Chunk* {.acyclic.} = ref object
    ## A chunk of bytecode with an interned string table.
    id*: int
    file*: string
    code*: seq[uint8]
    strings*: seq[string] = newSeqOfCap[string](64)
    stringIds: Table[string, uint16] = initTable[string, uint16](64)

  Script* {.acyclic.} = ref object
    mainChunk*: Chunk
    procs*: seq[Proc]

  CachedOps* = ref object
    opcodes*: seq[Opcode]
    arg1*: seq[int64]
    arg2*: seq[int64]
    jumpTargets*: seq[int]
    hot*: seq[int]
      ## Per-op back-edge execution counts (only jump-back sites use them).
    hotFired*: seq[bool]
      ## Whether `onHotLoop` already fired for each op index.

  Vm* {.acyclic.} = ref object
    globals*: Table[string, Value]
    opCache*: Table[int, CachedOps]
    onHotLoop*: proc(chunk: Chunk, pcIdx: int) {.closure.}
      ## Optional hook invoked once per loop site when its back-edge count
      ## reaches `SqlJitHotLoopThreshold`. Nil by default; a future JIT
      ## compiler can install it to compile hot loops natively while the
      ## interpreter remains the fallback.

const SqlJitHotLoopThreshold* = 50
  ## Back-edge executions after which a loop site is considered hot.

var nextChunkId {.global.}: int = 0

const MaxIds = int(high(uint16)) + 1

proc getString*(chunk: var Chunk, str: sink string): uint16 =
  ## O(1) string interning; returns the existing id or inserts.
  if chunk.stringIds.hasKey(str):
    return chunk.stringIds[str]
  if unlikely(chunk.strings.len >= MaxIds):
    raise newException(RangeDefect,
      "chunk string table overflow (uint16 id space exhausted)")
  let nextId = chunk.strings.len.uint16
  discard chunk.stringIds.hasKeyOrPut(str, nextId)
  chunk.strings.add(ensureMove(str))
  result = nextId

proc emit*(chunk: var Chunk, opc: Opcode) =
  chunk.code.add(opc.uint8)

proc emit*(chunk: var Chunk, u8: uint8) =
  chunk.code.add(u8)

proc emit*(chunk: var Chunk, u16: uint16) =
  chunk.code.add(uint8(u16 and 0x00ff))
  chunk.code.add(uint8((u16 shr 8) and 0x00ff))

proc emit*(chunk: var Chunk, val: int64) =
  chunk.code.add(cast[array[sizeof(int64), uint8]](val))

proc emit*(chunk: var Chunk, val: float64) =
  chunk.code.add(cast[array[sizeof(float64), uint8]](val))

proc fillHole*(chunk: var Chunk, hole: int, val: uint8) =
  chunk.code[hole] = val

proc fillHole*(chunk: var Chunk, hole: int, val: uint16) =
  chunk.code[hole] = uint8(val and 0x00ff)
  chunk.code[hole + 1] = uint8((val and 0xff00) shr 8)

proc newChunk*(file: string): Chunk =
  result = Chunk(file: file)
  result.id = atomicInc(nextChunkId)

proc newScript*(main: Chunk): Script =
  result = Script(mainChunk: main)

proc newVm*(): Vm =
  result = Vm()

proc hash*(x: Chunk): Hash =
  hash(x.id)

proc `==`*(a, b: Chunk): bool =
  if a.isNil and b.isNil: return true
  if a.isNil or b.isNil: return false
  a.id == b.id

proc `$`*(c: Chunk): string =
  if c == nil: return ""
  if c.file.len > 0: return c.file
  $c

proc parseChunk(chunk: Chunk): CachedOps =
  var
    opcodes: seq[Opcode]
    arg1: seq[int64]
    arg2: seq[int64]
    byteOffsets: seq[int]

  template addOp(oc: Opcode, a1: int64 = 0'i64, a2: int64 = 0'i64) =
    opcodes.add(oc)
    arg1.add(a1)
    arg2.add(a2)
    byteOffsets.add(opByteOffset)

  let codeLen = chunk.code.len
  var pc = 0
  while pc < codeLen:
    let opByteOffset = pc
    let oc = Opcode(chunk.code[pc])
    inc pc
    case oc
    of opcPushI:
      var raw: array[sizeof(int64), uint8]
      for i in 0 ..< sizeof(int64):
        raw[i] = chunk.code[pc + i]
      let v = cast[int64](raw)
      pc += sizeof(int64)
      addOp(oc, v)
    of opcPushF:
      var raw: array[sizeof(float64), uint8]
      for i in 0 ..< sizeof(float64):
        raw[i] = chunk.code[pc + i]
      let v = cast[float64](raw)
      pc += sizeof(float64)
      addOp(oc, cast[int64](v))
    of opcPushS, opcPushG:
      let sid = uint16(chunk.code[pc]) or (uint16(chunk.code[pc + 1]) shl 8)
      pc += 2
      addOp(oc, sid.int64)
    of opcPushL, opcPopL, opcIncL, opcDecL, opcDiscard:
      let idx = chunk.code[pc].int64
      pc += 1
      addOp(oc, idx)
    of opcJumpFwd, opcJumpFwdT, opcJumpFwdF, opcJumpBack:
      let dist = (uint16(chunk.code[pc]) or
        (uint16(chunk.code[pc + 1]) shl 8)).int64
      pc += 2
      addOp(oc, dist)
    of opcCallD:
      let fileSid = uint16(chunk.code[pc]) or
        (uint16(chunk.code[pc + 1]) shl 8)
      let pid = uint16(chunk.code[pc + 2]) or
        (uint16(chunk.code[pc + 3]) shl 8)
      pc += 4
      addOp(oc, fileSid.int64, pid.int64)
    of opcSqlCmp, opcSqlArith:
      let mode = chunk.code[pc].int64
      pc += 1
      addOp(oc, mode)
    of opcSqlAgg:
      let mode = chunk.code[pc].int64
      let slot = chunk.code[pc + 1].int64
      pc += 2
      addOp(oc, mode, slot)
    of opcPushTrue, opcPushFalse, opcPopG, opcEqI, opcInvB,
       opcSqlIsNull, opcHalt:
      addOp(oc)

  var jumpTargets = newSeq[int](opcodes.len)
  for i in 0 ..< jumpTargets.len:
    jumpTargets[i] = -1
  var byteToOp = newSeq[int](codeLen)
  for i in 0 ..< codeLen:
    byteToOp[i] = -1
  for i, b in byteOffsets:
    if b >= 0 and b < codeLen:
      byteToOp[b] = i
  for i, oc in opcodes:
    case oc
    of opcJumpFwd, opcJumpFwdT, opcJumpFwdF:
      let dist = arg1[i].int
      let operandStart = byteOffsets[i] + 1
      let targetByte = operandStart + (dist - 1)
      if targetByte >= 0 and targetByte < codeLen:
        jumpTargets[i] = byteToOp[targetByte]
    of opcJumpBack:
      let dist = arg1[i].int
      let targetByte = byteOffsets[i] - dist
      if targetByte >= 0 and targetByte < codeLen:
        jumpTargets[i] = byteToOp[targetByte]
    else: discard

  result = CachedOps(
    opcodes: opcodes,
    arg1: arg1,
    arg2: arg2,
    jumpTargets: jumpTargets,
    hot: newSeq[int](opcodes.len),
    hotFired: newSeq[bool](opcodes.len)
  )

proc getCachedOps*(vm: Vm, chunk: Chunk): CachedOps =
  if vm.opCache.hasKey(chunk.id):
    return vm.opCache[chunk.id]
  result = parseChunk(chunk)
  vm.opCache[chunk.id] = result

proc interpret*(vm: Vm, script: Script, startChunk: Chunk,
    args: openArray[Value] = []): Value {.discardable.} =
  ## Interpret `startChunk` in the context of `script`.
  var
    stack: Stack = newSeqOfCap[Value](64)
    stackBottom = 0
    callStack: seq[tuple[chunk: Chunk, ops: CachedOps, pc: int, sb: int]] = @[]
    curChunk = startChunk
    curScript = script
  for a in args:
    stack.add(a)
  var co = vm.getCachedOps(curChunk)
  var pcIdx = 0

  template ensureLocal(idx: int) =
    let needed = stackBottom + idx
    while stack.len <= needed:
      stack.add(initValue(999'i64))

  while pcIdx < co.opcodes.len:
    let oc = co.opcodes[pcIdx]
    case oc
    of opcPushI:
      stack.add(initValue(co.arg1[pcIdx]))
    of opcPushF:
      stack.add(initValue(cast[float64](co.arg1[pcIdx])))
    of opcPushS:
      let sid = co.arg1[pcIdx].uint16
      stack.add(initValue(curChunk.strings[sid]))
    of opcPushTrue:
      stack.add(initValue(true))
    of opcPushFalse:
      stack.add(initValue(false))
    of opcPushG:
      let sid = co.arg1[pcIdx].uint16
      stack.add(vm.globals[curChunk.strings[sid]])
    of opcPopG:
      let sid = co.arg1[pcIdx].uint16
      let val = stack.pop()
      vm.globals[curChunk.strings[sid]] = val
    of opcPushL:
      let idx = co.arg1[pcIdx].int
      ensureLocal(idx)
      stack.add(stack[stackBottom + idx])
    of opcPopL:
      let idx = co.arg1[pcIdx].int
      let val = stack.pop()
      ensureLocal(idx)
      stack[stackBottom + idx] = val
    of opcIncL:
      let idx = co.arg1[pcIdx].int
      ensureLocal(idx)
      stack[stackBottom + idx] =
        initValue(stack[stackBottom + idx].intVal + 1)
    of opcDecL:
      let idx = co.arg1[pcIdx].int
      ensureLocal(idx)
      stack[stackBottom + idx] =
        initValue(stack[stackBottom + idx].intVal - 1)
    of opcEqI:
      if stack.len < 2:
        raise newException(IndexDefect, "stack underflow in opcEqI")
      let b = stack[^1].intVal
      let a = stack[^2].intVal
      stack[^2] = initValue(a == b)
      stack.setLen(stack.len - 1)
    of opcInvB:
      if stack.len < 1:
        raise newException(IndexDefect, "stack underflow in opcInvB")
      stack[^1] = initValue(not stack[^1].boolVal)
    of opcJumpFwd:
      let tgt = co.jumpTargets[pcIdx]
      if tgt >= 0: pcIdx = tgt - 1
    of opcJumpFwdT:
      let val = stack[^1]
      let cond =
        case val.typeId
        of tyBool: val.boolVal
        of tyJsonStorage:
          val.jsonVal != nil and val.jsonVal.kind == JBool and
            val.jsonVal.bVal
        else: false
      if cond:
        let tgt = co.jumpTargets[pcIdx]
        if tgt >= 0: pcIdx = tgt - 1
    of opcJumpFwdF:
      let val = stack[^1]
      let cond =
        case val.typeId
        of tyBool: val.boolVal
        of tyJsonStorage:
          val.jsonVal != nil and val.jsonVal.kind == JBool and
            val.jsonVal.bVal
        else: false
      if not cond:
        let tgt = co.jumpTargets[pcIdx]
        if tgt >= 0: pcIdx = tgt - 1
    of opcJumpBack:
      inc co.hot[pcIdx]
      if co.hot[pcIdx] == SqlJitHotLoopThreshold and not co.hotFired[pcIdx]:
        co.hotFired[pcIdx] = true
        when defined(boogieSqlJitDebug):
          echo "[jit] hot loop at pc=", pcIdx, " chunk=", curChunk.file
        if vm.onHotLoop != nil:
          vm.onHotLoop(curChunk, pcIdx)
      let tgt = co.jumpTargets[pcIdx]
      if tgt >= 0: pcIdx = tgt - 1
      else: pcIdx = tgt - 1
    of opcCallD:
      let pid = co.arg2[pcIdx].int
      if pid < 0 or pid >= curScript.procs.len:
        raise newException(ValueError,
          "opcCallD: proc " & $pid & " not found (" &
            $curScript.procs.len & " procs)")
      let p = curScript.procs[pid]
      if p.kind != pkForeign:
        raise newException(ValueError,
          "native procs are not supported in the SQL VM")
      if stack.len < p.paramCount:
        raise newException(ValueError,
          "not enough arguments on stack for call to " & p.name &
            ": need " & $p.paramCount & ", have " & $stack.len)
      callStack.add((curChunk, co, pcIdx, stackBottom))
      stackBottom = stack.len - p.paramCount
      var callResult: Value = nil
      if p.paramCount > 0:
        callResult = p.foreign(
          cast[StackView](addr stack[stackBottom]), p.paramCount)
      else:
        callResult = p.foreign(nil, 0)
      stack.setLen(stackBottom)
      let frame = callStack.pop()
      curChunk = frame.chunk
      co = frame.ops
      pcIdx = frame.pc
      stackBottom = frame.sb
      if p.hasResult:
        stack.add(callResult)
    of opcDiscard:
      let n = co.arg1[pcIdx].int
      if n > 0 and stack.len >= n:
        stack.setLen(stack.len - n)
    of opcSqlCmp:
      let mode = co.arg1[pcIdx].int
      let b = stack.pop()
      let a = stack.pop()
      var res = false
      if a != nil and b != nil:
        template numOf(x: Value): float64 =
          if x.typeId == tyInt: x.intVal.float64 else: x.floatVal
        template apply(av: float64, bv: float64) =
          case mode
          of 0: res = av == bv
          of 1: res = av != bv
          of 2: res = av < bv
          of 3: res = av <= bv
          of 4: res = av > bv
          else: res = av >= bv
        template applyStr(av: string, bv: string) =
          case mode
          of 0: res = av == bv
          of 1: res = av != bv
          of 2: res = av < bv
          of 3: res = av <= bv
          of 4: res = av > bv
          else: res = av >= bv
        let aNum = a.typeId in {tyInt, tyFloat}
        let bNum = b.typeId in {tyInt, tyFloat}
        if aNum and bNum:
          apply(numOf(a), numOf(b))
        elif a.typeId == tyString and b.typeId == tyString:
          applyStr(a.stringVal[], b.stringVal[])
        elif a.typeId == tyBool and b.typeId == tyBool:
          let ai = int(a.boolVal)
          let bi = int(b.boolVal)
          case mode
          of 0: res = ai == bi
          of 1: res = ai != bi
          of 2: res = ai < bi
          of 3: res = ai <= bi
          of 4: res = ai > bi
          else: res = ai >= bi
        elif aNum and b.typeId == tyString:
          try: apply(numOf(a), parseFloat(b.stringVal[]))
          except ValueError: res = false
        elif a.typeId == tyString and bNum:
          try: apply(parseFloat(a.stringVal[]), numOf(b))
          except ValueError: res = false
      stack.add(initValue(res))
    of opcSqlIsNull:
      let a = stack.pop()
      stack.add(initValue(a == nil))
    of opcSqlArith:
      let mode = co.arg1[pcIdx].int
      let b = stack.pop()
      let a = stack.pop()
      if a == nil or b == nil:
        stack.add(nil)
      else:
        let aNum = a.typeId in {tyInt, tyFloat}
        let bNum = b.typeId in {tyInt, tyFloat}
        if not (aNum and bNum):
          stack.add(nil)
        elif a.typeId == tyInt and b.typeId == tyInt and mode != 3:
          let av = a.intVal
          let bv = b.intVal
          case mode
          of 0: stack.add(initValue(av + bv))
          of 1: stack.add(initValue(av - bv))
          else: stack.add(initValue(av * bv))
        else:
          let av = if a.typeId == tyInt: a.intVal.float64 else: a.floatVal
          let bv = if b.typeId == tyInt: b.intVal.float64 else: b.floatVal
          case mode
          of 0: stack.add(initValue(av + bv))
          of 1: stack.add(initValue(av - bv))
          of 2: stack.add(initValue(av * bv))
          else:
            if bv == 0.0: stack.add(nil)
            else: stack.add(initValue(av / bv))
    of opcSqlAgg:
      let mode = co.arg1[pcIdx].int
      let slot = co.arg2[pcIdx].int
      ensureLocal(slot + 1)
      let v = stack.pop()
      template numOf(x: Value): float64 =
        if x.typeId == tyInt: x.intVal.float64 else: x.floatVal
      template cellLess(a, b: Value): bool =
        if b == nil: false
        elif a == nil: true
        elif a.typeId in {tyInt, tyFloat} and
            b.typeId in {tyInt, tyFloat}:
          numOf(a) < numOf(b)
        elif a.typeId == tyString and b.typeId == tyString:
          a.stringVal[] < b.stringVal[]
        else: false
      var acc = stack[stackBottom + slot]
      case mode
      of 0:
        let n = (if acc != nil and acc.typeId == tyInt: acc.intVal
                 else: 0'i64) +
                (if v != nil and v.typeId == tyInt: v.intVal else: 0'i64)
        stack[stackBottom + slot] = initValue(n)
      of 4:
        if v != nil:
          let n = (if acc != nil and acc.typeId == tyInt: acc.intVal
                   else: 0'i64) + 1
          stack[stackBottom + slot] = initValue(n)
      of 1, 5:
        if v != nil and v.typeId in {tyInt, tyFloat}:
          if acc == nil or acc.typeId notin {tyInt, tyFloat}:
            stack[stackBottom + slot] = initValue(numOf(v))
          elif acc.typeId == tyInt and v.typeId == tyInt:
            stack[stackBottom + slot] = initValue(acc.intVal + v.intVal)
          else:
            stack[stackBottom + slot] = initValue(numOf(acc) + numOf(v))
          if mode == 5:
            let cnt = stack[stackBottom + slot + 1]
            let c = if cnt != nil and cnt.typeId == tyInt: cnt.intVal
                    else: 0'i64
            stack[stackBottom + slot + 1] = initValue(c + 1)
      of 2, 3:
        if v != nil:
          if acc == nil or acc.typeId == tyNil:
            stack[stackBottom + slot] = v
          else:
            let smaller = cellLess(v, acc)
            if (mode == 2 and smaller) or (mode == 3 and not smaller):
              stack[stackBottom + slot] = v
      else:
        discard
    of opcHalt:
      break
    inc pcIdx
  if stack.len > 0:
    return stack[^1]
  else:
    return Value(typeId: tyNil)
