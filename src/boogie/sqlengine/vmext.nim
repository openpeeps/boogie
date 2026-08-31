# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import pkg/voodoo/extensibles
import pkg/vancode/interpreter/value

## SQL evaluation opcodes for the vancode VM, installed via the same voodoo
## extension mechanism the Tim engine uses for its HTML opcodes.
##
## IMPORTANT: this module must be imported BEFORE the first import of
## `pkg/vancode/interpreter/vm` in a program, because the extensions are
## spliced into the VM when it is compiled. `boogie/sqlengine` handles the
## import order; import that module instead of this one.
##
## The opcodes here are deliberately self-contained (they operate purely on
## the VM stack) because extension bodies resolve their symbols in the VM
## module's scope:
##
## - `opcSqlCmp <mode>`: pops right then left and pushes a bool. Unlike the
##   built-in relational opcodes this one understands SQL NULL (nil values),
##   mixed int/float numerics and full string ordering. Modes:
##   0 = eq, 1 = ne, 2 = lt, 3 = le, 4 = gt, 5 = ge.
##   Per SQL semantics any comparison involving NULL yields false.
## - `opcSqlIsNull`: pops a value and pushes whether it is SQL NULL.
## - `opcSqlArith <mode>`: pops right then left and pushes the numeric result,
##   preserving SQL NULL propagation (NULL in, NULL out) and returning NULL on
##   division by zero. Mixed int/float operands promote to float. Modes:
##   0 = add, 1 = sub, 2 = mul, 3 = div.
## - `opcSqlAgg <mode> <localSlot>`: pops a value and folds it into the
##   accumulator held in a local slot, implementing the aggregate step for
##   GROUP-less aggregation. Modes: 0 = count-star (adds the popped int),
##   1 = sum (numeric, NULL-skipping), 2 = min, 3 = max, 4 = count-non-null,
##   5 = avg-pair (slot holds the running sum while slot+1 counts rows).
##
## Table access itself is bridged through foreign procs registered by
## `boogie/sqlengine`, which can legally capture the connection object.

proc isSqlNull*(v: Value): bool =
  ## True when the VM value represents SQL NULL (an absent cell). NULL is
  ## represented as a plain nil `Value` ref, which never appears as a legal
  ## stack value otherwise.
  v == nil

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

block extendVmWithSqlOpcodes:
  extendEnum Opcode:
    opcSqlCmp = "sqlCmp"
    opcSqlIsNull = "sqlIsNull"
    opcSqlArith = "sqlArith"
    opcSqlAgg = "sqlAgg"

  extendCaseStmt "vmParseChunkCase":
    case oc:
    of opcSqlCmp:
      let mode = readArg[uint8](pc)
      addOp(oc, mode.int64, 0, akInt)
    of opcSqlArith:
      let mode = readArg[uint8](pc)
      addOp(oc, mode.int64, 0, akInt)
    of opcSqlAgg:
      let mode = readArg[uint8](pc)
      let slot = readArg[uint8](pc)
      addOp(oc, mode.int64, slot.int64, akInt, akInt)
    of opcSqlIsNull:
      addOp(oc)

  extendCaseStmt "vmInterpretCase":
    case oc:
    of opcSqlCmp:
      # pops right, then left; pushes the comparison result as a bool.
      # Mirrors SQLite column-affinity behavior: comparing a number against
      # a text operand coerces the text to a number when it parses.
      let mode = co.getArg1Int(pcIdx)
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
      stack.push(initValue(res))
    of opcSqlIsNull:
      let a = stack.pop()
      stack.push(initValue(a == nil))
    of opcSqlArith:
      # pops right, then left; NULL propagates, division by zero yields NULL
      let mode = co.getArg1Int(pcIdx)
      let b = stack.pop()
      let a = stack.pop()
      if a == nil or b == nil:
        stack.push(nil)
      else:
        let aNum = a.typeId in {tyInt, tyFloat}
        let bNum = b.typeId in {tyInt, tyFloat}
        if not (aNum and bNum):
          stack.push(nil)
        elif a.typeId == tyInt and b.typeId == tyInt and mode != 3:
          let av = a.intVal
          let bv = b.intVal
          case mode
          of 0: stack.push(initValue(av + bv))
          of 1: stack.push(initValue(av - bv))
          else: stack.push(initValue(av * bv))
        else:
          let av = if a.typeId == tyInt: a.intVal.float64 else: a.floatVal
          let bv = if b.typeId == tyInt: b.intVal.float64 else: b.floatVal
          case mode
          of 0: stack.push(initValue(av + bv))
          of 1: stack.push(initValue(av - bv))
          of 2: stack.push(initValue(av * bv))
          else:
            if bv == 0.0: stack.push(nil)
            else: stack.push(initValue(av / bv))
    of opcSqlAgg:
      # folds the popped value into an accumulator local (GROUP-less aggregates)
      let mode = co.getArg1Int(pcIdx)
      let slot = co.arg2[pcIdx].int
      when defined(boogieSqlDebug):
        echo "[agg] mode=", mode, " slot=", slot, " stacklen=", stack.len
      ensureLocal(slot + 1)
      let v = stack.pop()
      template numOf(x: Value): float64 =
        if x.typeId == tyInt: x.intVal.float64 else: x.floatVal
      template cellLess(a, b: Value): bool =
        ## total ordering consistent across kinds (mirrors opcSqlCmp rules)
        if b == nil: false
        elif a == nil: true
        elif a.typeId in {tyInt, tyFloat} and b.typeId in {tyInt, tyFloat}:
          numOf(a) < numOf(b)
        elif a.typeId == tyString and b.typeId == tyString:
          a.stringVal[] < b.stringVal[]
        else: false
      var acc = stack[stackBottom + slot]
      case mode
      of 0:
        # count-star: the compiler pushes a literal 1 per row
        let n = (if acc != nil and acc.typeId == tyInt: acc.intVal else: 0'i64) +
                (if v != nil and v.typeId == tyInt: v.intVal else: 0'i64)
        stack[stackBottom + slot] = initValue(n)
      of 4:
        # count over non-null values only
        if v != nil:
          let n = (if acc != nil and acc.typeId == tyInt: acc.intVal else: 0'i64) + 1
          stack[stackBottom + slot] = initValue(n)
      of 1, 5:
        # running sum; mode 5 also counts rows into slot+1 (for AVG)
        if v != nil and v.typeId in {tyInt, tyFloat}:
          if acc == nil or acc.typeId notin {tyInt, tyFloat}:
            stack[stackBottom + slot] = initValue(numOf(v))
          elif acc.typeId == tyInt and v.typeId == tyInt:
            stack[stackBottom + slot] = initValue(acc.intVal + v.intVal)
          else:
            stack[stackBottom + slot] = initValue(numOf(acc) + numOf(v))
          if mode == 5:
            let cnt = stack[stackBottom + slot + 1]
            let c = if cnt != nil and cnt.typeId == tyInt: cnt.intVal else: 0'i64
            stack[stackBottom + slot + 1] = initValue(c + 1)
      of 2, 3:
        # MIN / MAX over ordered cells; NULL inputs are skipped
        if v != nil:
          if acc == nil or acc.typeId == tyNil:
            stack[stackBottom + slot] = v
          else:
            let smaller = cellLess(v, acc)
            if (mode == 2 and smaller) or (mode == 3 and not smaller):
              stack[stackBottom + slot] = v
      else:
        discard
