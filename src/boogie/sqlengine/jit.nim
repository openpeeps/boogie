# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

## Native predicate JIT for the SQL engine (dynasm-backed).
##
## Compiles hot WHERE-clause shapes to straight-line machine code on both
## x64 and AArch64. The kernel operates purely on unboxed int64 slots plus
## a parallel null-flag array, so generated code never touches GC-managed
## references: the calling foreign proc unboxes `Value`s on entry and
## re-boxes the boolean result on exit.
##
## Supported subset (anything else declines to the interpreter, which stays
## the source of truth for errors):
##
## - integer / boolean columns and integer / boolean literals
## - comparisons `= <> < <= > >=` over int/int and bool/bool
## - arithmetic `+ - *` over integers (division always declines: the
##   interpreter promotes `/` to float, which x64 dynasm cannot encode;
##   floats anywhere decline for the same reason)
## - `AND` / `OR` over statically boolean operands, `IS [NOT] NULL`
## - `NULL` literals (comparison with NULL is always false, arithmetic
##   with NULL is always NULL, mirroring `opcSqlCmp` / `opcSqlArith`)
##
## NOT, placeholders, floats, strings and unknown nodes decline.
## Integer overflow raises `OverflowDefect` exactly like the interpreter
## when overflow checks are enabled, and wraps otherwise
## (`compileOption("overflowChecks")` gates the checked paths).

import std/[strutils, options]
import pkg/dynasm
import pkg/openparser/sql
import ../stores/rdbms

var jitEnabled* = true
  ## Master switch for JIT predicate emission. Tests toggle this for
  ## interpreter-vs-JIT A/B comparisons within one binary.

type
  JitPredFn* = proc(slots, nulls, err: pointer): int64 {.cdecl.}
    ## Kernel ABI: flat int64 value slots, parallel int64 null flags
    ## (nonzero = NULL), error flag slot. Returns 0/1. On integer overflow
    ## the kernel stores nonzero into `err` and returns 0; the caller then
    ## raises `OverflowDefect` to mirror the interpreter.

  JitPred* = ref object
    cols*: seq[string]
      ## Referenced columns in first-appearance order; kernel arg i.
    constVals*: seq[int64]
    constNulls*: seq[int64]
    nslots*: int
      ## cols + consts + eval-stack scratch.
    buf*: ExecutableBuffer
      ## Owns the machine code; keep alive as long as `fn` is used.
    fn*: JitPredFn

  JitTy = enum jtInt, jtBool

  JitNodeKind = enum jCol, jConst, jCmp, jArith, jAnd, jOr, jIsNull

  JitNode = ref object
    ty: JitTy
    isNullConst: bool
    case kind: JitNodeKind
    of jCol:
      colIdx: int
      colTy: JitTy
    of jConst:
      cVal: int64
        ## Index into the const pool (value lives in a slot).
    of jCmp:
      cmpMode: int
      cmpLeft, cmpRight: JitNode
    of jArith:
      arithMode: int
      arithLeft, arithRight: JitNode
    of jAnd, jOr:
      bLeft, bRight: JitNode
    of jIsNull:
      operand: JitNode
      negated: bool

const
  JitCmpEq = 0
  JitCmpNe = 1
  JitCmpLt = 2
  JitCmpLe = 3
  JitCmpGt = 4
  JitCmpGe = 5

  JitArithAdd = 0
  JitArithSub = 1
  JitArithMul = 2

  JitMaxDepth = 48
    ## Deeper expressions stay on the interpreter.

proc jitIdent(n: SqlNode): string =
  case n.kind
  of nkIdent, nkQuotedIdent, nkIntegerLit, nkNumericLit, nkStringLit, nkRaw:
    n.strVal
  else:
    ""

type JitBuilder = object
  tbl: DbTable
  cols: seq[string]
  colTys: seq[JitTy]
  constVals: seq[int64]
  constNulls: seq[int64]
  depth: int
  failed: bool

proc colIdx(b: var JitBuilder, name: string, ty: JitTy): int =
  for i, c in b.cols:
    if c == name:
      if b.colTys[i] != ty:
        b.failed = true
        return -1
      return i
  b.cols.add(name)
  b.colTys.add(ty)
  b.cols.len - 1

proc constIdx(b: var JitBuilder, val: int64, isNull: bool): int =
  b.constVals.add(val)
  b.constNulls.add(if isNull: 1'i64 else: 0'i64)
  b.constVals.len - 1

proc build(b: var JitBuilder, n: SqlNode): JitNode

proc buildBoolOperand(b: var JitBuilder, n: SqlNode): JitNode =
  ## Builds an operand that must be statically boolean; nil means decline.
  let x = b.build(n)
  if x == nil or x.ty != jtBool or x.isNullConst:
    return nil
  x

proc build(b: var JitBuilder, n: SqlNode): JitNode =
  if n.isNil or b.failed:
    return nil
  inc b.depth
  if b.depth > JitMaxDepth:
    b.failed = true
    dec b.depth
    return nil
  template decline(): JitNode =
    dec b.depth
    nil
  case n.kind
  of nkIntegerLit:
    try:
      let v = n.strVal.parseBiggestInt.int64
      let ci = b.constIdx(v, false)
      result = JitNode(kind: jConst, ty: jtInt, cVal: int64(ci))
    except ValueError:
      # Same call the interpreter lowering makes; let it raise there.
      return decline()
  of nkNumericLit, nkStringLit:
    # Floats decline (x64 dynasm has no SSE encoding); strings decline.
    return decline()
  of nkIdent:
    case n.strVal.toLowerAscii()
    of "null":
      let ci = b.constIdx(0'i64, true)
      result = JitNode(kind: jConst, ty: jtInt, isNullConst: true,
        cVal: int64(ci))
    of "true":
      let ci = b.constIdx(1'i64, false)
      result = JitNode(kind: jConst, ty: jtBool, cVal: int64(ci))
    of "false":
      let ci = b.constIdx(0'i64, false)
      result = JitNode(kind: jConst, ty: jtBool, cVal: int64(ci))
    else:
      let def = b.tbl.findColumn(n.strVal)
      if def.isNone:
        # Unknown column: decline so lowering raises the exact error.
        return decline()
      case def.get().kind
      of dtInt:
        let i = b.colIdx(n.strVal, jtInt)
        if i < 0: return decline()
        result = JitNode(kind: jCol, ty: jtInt, colIdx: i, colTy: jtInt)
      of dtBool:
        let i = b.colIdx(n.strVal, jtBool)
        if i < 0: return decline()
        result = JitNode(kind: jCol, ty: jtBool, colIdx: i, colTy: jtBool)
      else:
        return decline()
  of nkPlaceholder:
    # Runtime type unknown; keep on the interpreter.
    return decline()
  of nkDot:
    if n.len < 2: return decline()
    result = b.build(n[1])
  of nkSelectPair, nkPrGroup:
    if n.len < 1: return decline()
    result = b.build(n[0])
  of nkPrefix:
    if n.len < 2: return decline()
    case n[0].strVal.toLowerAscii()
    of "-":
      let x = b.build(n[1])
      if x == nil: return decline()
      if x.isNullConst:
        result = x
      elif x.ty == jtInt:
        let zc = b.constIdx(0'i64, false)
        let z = JitNode(kind: jConst, ty: jtInt, cVal: int64(zc))
        result = JitNode(kind: jArith, ty: jtInt, arithMode: JitArithSub,
          arithLeft: z, arithRight: x)
      else:
        # Negating a bool is NULL at runtime; decline to preserve the
        # exact interpreter path.
        return decline()
    of "+":
      result = b.build(n[1])
    else:
      # NOT and anything else decline.
      return decline()
  of nkInfix:
    if n.len < 3: return decline()
    let op = n[0].strVal.toLowerAscii()
    case op
    of "=", "!=", "<>", "<", "<=", ">", ">=":
      var lhs = n[1]
      var rhs = n[2]
      var negate = false
      while lhs.kind == nkPrefix and lhs.len >= 2 and
            lhs[0].strVal.toLowerAscii() == "not":
        negate = not negate
        lhs = lhs[1]
      while rhs.kind == nkPrefix and rhs.len >= 2 and
            rhs[0].strVal.toLowerAscii() == "not":
        negate = not negate
        rhs = rhs[1]
      let a = b.build(lhs)
      let r = b.build(rhs)
      if a == nil or r == nil: return decline()
      if a.isNullConst or r.isNullConst:
        # Comparison with NULL is always false (both polarities: the
        # interpreter yields false for `= NULL` and `<> NULL` alike).
        let ci = b.constIdx(0'i64, false)
        result = JitNode(kind: jConst, ty: jtBool, cVal: int64(ci))
      elif (a.ty == jtInt and r.ty == jtInt) or
           (a.ty == jtBool and r.ty == jtBool):
        var mode =
          case op
          of "=": JitCmpEq
          of "<": JitCmpLt
          of "<=": JitCmpLe
          of ">": JitCmpGt
          of ">=": JitCmpGe
          else: JitCmpNe
        if negate:
          mode =
            case mode
            of JitCmpEq: JitCmpNe
            of JitCmpNe: JitCmpEq
            of JitCmpLt: JitCmpGe
            of JitCmpLe: JitCmpGt
            of JitCmpGt: JitCmpLe
            else: JitCmpLt
        result = JitNode(kind: jCmp, ty: jtBool, cmpMode: mode,
          cmpLeft: a, cmpRight: r)
      else:
        return decline()
    of "and", "or":
      let a = b.buildBoolOperand(n[1])
      let r = b.buildBoolOperand(n[2])
      if a == nil or r == nil: return decline()
      if op == "and":
        result = JitNode(kind: jAnd, ty: jtBool, bLeft: a, bRight: r)
      else:
        result = JitNode(kind: jOr, ty: jtBool, bLeft: a, bRight: r)
    of "+", "-", "*":
      let a = b.build(n[1])
      let r = b.build(n[2])
      if a == nil or r == nil: return decline()
      if a.isNullConst or r.isNullConst:
        result = JitNode(kind: jConst, ty: jtInt, isNullConst: true,
          cVal: int64(b.constIdx(0'i64, true)))
      elif a.ty == jtInt and r.ty == jtInt:
        let mode =
          case op
          of "+": JitArithAdd
          of "-": JitArithSub
          else: JitArithMul
        result = JitNode(kind: jArith, ty: jtInt, arithMode: mode,
          arithLeft: a, arithRight: r)
      else:
        return decline()
    of "is":
      if jitIdent(n[2]).toUpperAscii() != "NULL": return decline()
      let x = b.build(n[1])
      if x == nil: return decline()
      result = JitNode(kind: jIsNull, ty: jtBool, operand: x,
        negated: false)
    of "is not":
      if jitIdent(n[2]).toUpperAscii() != "NULL": return decline()
      let x = b.build(n[1])
      if x == nil: return decline()
      result = JitNode(kind: jIsNull, ty: jtBool, operand: x,
        negated: true)
    else:
      return decline()
  else:
    return decline()
  dec b.depth
  if b.failed: return nil

proc tryBuildPred*(whereNode: SqlNode, tbl: DbTable): tuple[ok: bool,
    cols: seq[string], constVals, constNulls: seq[int64], root: JitNode,
    maxDepth: int] =
  ## Analyzes a WHERE clause for JIT compilation. `ok` is false when the
  ## shape falls outside the compilable subset (the interpreter then
  ## handles it, including all error paths).
  var b = JitBuilder(tbl: tbl)
  let root = b.build(whereNode)
  if b.failed or root == nil or root.ty != jtBool:
    return (false, @[], @[], @[], nil, 0)
  proc depth(n: JitNode): int =
    case n.kind
    of jCmp: 1 + max(depth(n.cmpLeft), depth(n.cmpRight) + 1)
    of jArith: 1 + max(depth(n.arithLeft), depth(n.arithRight) + 1)
    of jAnd, jOr: 1 + max(depth(n.bLeft), depth(n.bRight) + 1)
    of jIsNull: 1 + depth(n.operand)
    else: 1
  let d = depth(root)
  if d > JitMaxDepth:
    return (false, @[], @[], @[], nil, 0)
  (true, b.cols, b.constVals, b.constNulls, root, d)

# ---------------------------------------------------------------------------
# Code generation: stack machine over unboxed slots.
#
# Conventions: leaves (value, null) in (r10, r11) / (x9, x10); binary ops
# spill the left side to the scratch area and hold the right side in
# (rcx, rdx) / (x11, x12). Slot `i` lives at [base + i*8].
# ---------------------------------------------------------------------------

type JitGen = object
  ncols: int
  base: int
    ## First scratch slot index (cols + consts).
  sp: int
    ## Compile-time eval-stack depth.
  labels: int

proc newLabel(g: var JitGen, tag: string): string =
  inc g.labels
  "j" & $g.labels & "_" & tag

proc slotAbs(g: JitGen, idx: int): int = idx * 8

proc scratchAbs(g: JitGen): int = (g.base + g.sp) * 8

# --- x64 ---

proc xPush(a: var Assembler, g: var JitGen) =
  ## Spill (r10, r11) to the scratch area.
  let d = g.scratchAbs()
  dynasm a:
    arch x64
    mov [rbx + d], r10
    mov [r12 + d], r11
  inc g.sp

proc xPopSecond(a: var Assembler, g: var JitGen) =
  ## Pop into (rcx, rdx).
  dec g.sp
  let d = g.scratchAbs()
  dynasm a:
    arch x64
    mov rcx, [rbx + d]
    mov rdx, [r12 + d]
proc lab(a: var Assembler, name: string) =
  ## Binds a dynamically generated label at the current position.
  a.localLabel(name)

proc genNodeX64(a: var Assembler, g: var JitGen, n: JitNode) =
  case n.kind
  of jCol:
    let d = g.slotAbs(n.colIdx)
    dynasm a:
      arch x64
      mov r10, [rbx + d]
      mov r11, [r12 + d]
  of jConst:
    let d = g.slotAbs(g.ncols + int(n.cVal))
    dynasm a:
      arch x64
      mov r10, [rbx + d]
      mov r11, [r12 + d]
  of jCmp:
    genNodeX64(a, g, n.cmpLeft)
    xPush(a, g)
    genNodeX64(a, g, n.cmpRight)
    xPopSecond(a, g)
    let lf = g.newLabel("cf")
    let ld = g.newLabel("cd")
    dynasm a:
      arch x64
      test r11, r11
      jnz fwd(lf)
      test rdx, rdx
      jnz fwd(lf)
      cmp rcx, r10
    case n.cmpMode
    of JitCmpEq:
      dynasm a:
        arch x64
        sete al
    of JitCmpNe:
      dynasm a:
        arch x64
        setne al
    of JitCmpLt:
      dynasm a:
        arch x64
        setl al
    of JitCmpLe:
      dynasm a:
        arch x64
        setle al
    of JitCmpGt:
      dynasm a:
        arch x64
        setg al
    else:
      dynasm a:
        arch x64
        setge al
    dynasm a:
      arch x64
      movzx r10d, al
      `xor` r11d, r11d
      jmp fwd(ld)
    a.lab(lf)
    dynasm a:
      arch x64
      `xor` r10d, r10d
      `xor` r11d, r11d
    a.lab(ld)
  of jArith:
    genNodeX64(a, g, n.arithLeft)
    xPush(a, g)
    genNodeX64(a, g, n.arithRight)
    xPopSecond(a, g)
    let ln = g.newLabel("an")
    let ld = g.newLabel("ad")
    dynasm a:
      arch x64
      test r11, r11
      jnz fwd(ln)
      test rdx, rdx
      jnz fwd(ln)
      mov rax, rcx
    case n.arithMode
    of JitArithAdd:
      dynasm a:
        arch x64
        add rax, r10
    of JitArithSub:
      dynasm a:
        arch x64
        sub rax, r10
    else:
      dynasm a:
        arch x64
        imul rax, r10
    when compileOption("overflowChecks"):
      dynasm a:
        arch x64
        jo fwd("jit_ovf")
    dynasm a:
      arch x64
      mov r10, rax
      `xor` r11d, r11d
      jmp fwd(ld)
    a.lab(ln)
    dynasm a:
      arch x64
      `xor` r10d, r10d
      mov r11d, 1
    a.lab(ld)
  of jAnd, jOr:
    genNodeX64(a, g, n.bLeft)
    let lf = g.newLabel("bf")
    let ld = g.newLabel("bd")
    if n.kind == jAnd:
      # Falsy (or NULL) left short-circuits to false.
      dynasm a:
        arch x64
        test r11, r11
        jnz fwd(lf)
        test r10, r10
        jz fwd(lf)
      genNodeX64(a, g, n.bRight)
      dynasm a:
        arch x64
        test r11, r11
        jnz fwd(lf)
        test r10, r10
        jz fwd(lf)
        mov r10d, 1
        `xor` r11d, r11d
        jmp fwd(ld)
      a.lab(lf)
      dynasm a:
        arch x64
        `xor` r10d, r10d
        `xor` r11d, r11d
      a.lab(ld)
    else:
      # Truthy left short-circuits to true; otherwise evaluate right.
      let lr = g.newLabel("br")
      dynasm a:
        arch x64
        test r11, r11
        jnz fwd(lr)
        test r10, r10
        jz fwd(lr)
        mov r10d, 1
        `xor` r11d, r11d
        jmp fwd(ld)
      a.lab(lr)
      genNodeX64(a, g, n.bRight)
      dynasm a:
        arch x64
        test r11, r11
        jnz fwd(lf)
        test r10, r10
        jz fwd(lf)
        mov r10d, 1
        `xor` r11d, r11d
        jmp fwd(ld)
      a.lab(lf)
      dynasm a:
        arch x64
        `xor` r10d, r10d
        `xor` r11d, r11d
      a.lab(ld)
  of jIsNull:
    genNodeX64(a, g, n.operand)
    if n.negated:
      dynasm a:
        arch x64
        test r11, r11
        setz al
        movzx r10d, al
        `xor` r11d, r11d
    else:
      dynasm a:
        arch x64
        test r11, r11
        setnz al
        movzx r10d, al
        `xor` r11d, r11d

proc rPush(a: var Assembler, g: var JitGen) =
  ## Spill (x9, x10) to the scratch area.
  let d = g.scratchAbs()
  dynasm a:
    arch aarch64
    str x9, [x0, d]
    str x10, [x1, d]
  inc g.sp

proc rPopSecond(a: var Assembler, g: var JitGen) =
  ## Pop into (x11, x12).
  dec g.sp
  let d = g.scratchAbs()
  dynasm a:
    arch aarch64
    ldr x11, [x0, d]
    ldr x12, [x1, d]

proc genNodeArm(a: var Assembler, g: var JitGen, n: JitNode) =
  case n.kind
  of jCol:
    let d = g.slotAbs(n.colIdx)
    dynasm a:
      arch aarch64
      ldr x9, [x0, d]
      ldr x10, [x1, d]
  of jConst:
    let d = g.slotAbs(g.ncols + int(n.cVal))
    dynasm a:
      arch aarch64
      ldr x9, [x0, d]
      ldr x10, [x1, d]
  of jCmp:
    genNodeArm(a, g, n.cmpLeft)
    rPush(a, g)
    genNodeArm(a, g, n.cmpRight)
    rPopSecond(a, g)
    let lf = g.newLabel("cf")
    let ld = g.newLabel("cd")
    dynasm a:
      arch aarch64
      cmp x10, 0
      b.ne fwd(lf)
      cmp x12, 0
      b.ne fwd(lf)
      cmp x9, x11
    case n.cmpMode
    of JitCmpEq:
      dynasm a:
        arch aarch64
        cset w10, eq
    of JitCmpNe:
      dynasm a:
        arch aarch64
        cset w10, ne
    of JitCmpLt:
      dynasm a:
        arch aarch64
        cset w10, lt
    of JitCmpLe:
      dynasm a:
        arch aarch64
        cset w10, le
    of JitCmpGt:
      dynasm a:
        arch aarch64
        cset w10, gt
    else:
      dynasm a:
        arch aarch64
        cset w10, ge
    dynasm a:
      arch aarch64
      mov x11, xzr
      b fwd(ld)
    a.lab(lf)
    dynasm a:
      arch aarch64
      mov x10, xzr
      mov x11, xzr
    a.lab(ld)
    dynasm a:
      arch aarch64
      mov x9, x10
      mov x10, x11
  of jArith:
    genNodeArm(a, g, n.arithLeft)
    rPush(a, g)
    genNodeArm(a, g, n.arithRight)
    rPopSecond(a, g)
    let ln = g.newLabel("an")
    let ld = g.newLabel("ad")
    dynasm a:
      arch aarch64
      cmp x10, 0
      b.ne fwd(ln)
      cmp x12, 0
      b.ne fwd(ln)
    when compileOption("overflowChecks"):
      case n.arithMode
      of JitArithAdd:
        dynasm a:
          arch aarch64
          adds x9, x9, x11
          b.vs fwd("jit_ovf")
      of JitArithSub:
        dynasm a:
          arch aarch64
          subs x9, x9, x11
          b.vs fwd("jit_ovf")
      else:
        # 64-bit multiply overflow without smulh: reject the INT64_MIN
        # corner explicitly, then verify via an sdiv round-trip.
        let lok = g.newLabel("mok")
        let lchk = g.newLabel("mchk")
        dynasm a:
          arch aarch64
          movn x13, 0
          movz x14, 0
          movk x14, 0x8000, lsl, 48
          mul x15, x9, x11
          cmp x9, 0
          b.eq fwd(lok)
          cmp x9, x13
          b.ne fwd(lchk)
          cmp x11, x14
          b.eq fwd("jit_ovf")
        a.lab(lchk)
        dynasm a:
          arch aarch64
          sdiv x13, x15, x9
          cmp x13, x11
          b.ne fwd("jit_ovf")
        a.lab(lok)
        dynasm a:
          arch aarch64
          mov x9, x15
    else:
      case n.arithMode
      of JitArithAdd:
        dynasm a:
          arch aarch64
          add x9, x9, x11
      of JitArithSub:
        dynasm a:
          arch aarch64
          sub x9, x9, x11
      else:
        dynasm a:
          arch aarch64
          mul x9, x9, x11
    dynasm a:
      arch aarch64
      mov x10, xzr
      b fwd(ld)
    a.lab(ln)
    dynasm a:
      arch aarch64
      mov x9, xzr
      mov x10, 1
    a.lab(ld)
  of jAnd, jOr:
    genNodeArm(a, g, n.bLeft)
    let lf = g.newLabel("bf")
    let ld = g.newLabel("bd")
    if n.kind == jAnd:
      # Falsy (or NULL) left short-circuits to false.
      dynasm a:
        arch aarch64
        cmp x10, 0
        b.ne fwd(lf)
        cmp x9, 0
        b.eq fwd(lf)
      genNodeArm(a, g, n.bRight)
      dynasm a:
        arch aarch64
        cmp x10, 0
        b.ne fwd(lf)
        cmp x9, 0
        b.eq fwd(lf)
        mov x9, 1
        mov x10, xzr
        b fwd(ld)
      a.lab(lf)
      dynasm a:
        arch aarch64
        mov x9, xzr
        mov x10, xzr
      a.lab(ld)
    else:
      # Truthy left short-circuits to true; otherwise evaluate right.
      let lr = g.newLabel("br")
      dynasm a:
        arch aarch64
        cmp x10, 0
        b.ne fwd(lr)
        cmp x9, 0
        b.eq fwd(lr)
        mov x9, 1
        mov x10, xzr
        b fwd(ld)
      a.lab(lr)
      genNodeArm(a, g, n.bRight)
      dynasm a:
        arch aarch64
        cmp x10, 0
        b.ne fwd(lf)
        cmp x9, 0
        b.eq fwd(lf)
        mov x9, 1
        mov x10, xzr
        b fwd(ld)
      a.lab(lf)
      dynasm a:
        arch aarch64
        mov x9, xzr
        mov x10, xzr
      a.lab(ld)
  of jIsNull:
    genNodeArm(a, g, n.operand)
    if n.negated:
      dynasm a:
        arch aarch64
        cmp x10, 0
        cset w9, eq
        mov x10, xzr
    else:
      dynasm a:
        arch aarch64
        cmp x10, 0
        cset w9, ne
        mov x10, xzr

proc compilePred*(cols: seq[string], constVals, constNulls: seq[int64],
    root: JitNode, maxDepth: int): JitPred =
  ## Emits a straight-line predicate kernel for the current host.
  ## Signature: `fn(slots, nulls, err: pointer): int64`; slots hold
  ## cols ++ consts ++ scratch, nulls is parallel, `err` receives nonzero
  ## on integer overflow. The result is left collapsed to 0/1 in the
  ## return register.
  let nslots = cols.len + constVals.len + maxDepth + 1
  when defined(arm64) or defined(aarch64):
    var a = newAssembler(akAarch64)
    var g = JitGen(ncols: cols.len, base: cols.len + constVals.len)
    genNodeArm(a, g, root)
    dynasm a:
      arch aarch64
      cmp x10, 0
      b.ne fwd("jit_z")
      cmp x9, 0
      b.eq fwd("jit_z")
      mov x0, 1
      b fwd("jit_done")
    a.lab("jit_z")
    dynasm a:
      arch aarch64
      mov x0, xzr
    a.lab("jit_done")
    dynasm a:
      arch aarch64
      ret()
    a.lab("jit_ovf")
    dynasm a:
      arch aarch64
      mov x9, 1
      str x9, [x2]
      mov x0, xzr
      ret()
    var buf = a.finalize()
    result = JitPred(cols: cols, constVals: constVals,
      constNulls: constNulls, nslots: nslots, buf: buf)
    result.fn = cast[JitPredFn](result.buf.ptrAt(AssemblyOffset(0)))
  else:
    var a = newAssembler(akX64)
    var g = JitGen(ncols: cols.len, base: cols.len + constVals.len)
    when defined(windows):
      dynasm a:
        arch x64
        push rbx
        push r12
        push r13
        mov rbx, rcx
        mov r12, rdx
        mov r13, r8
    else:
      dynasm a:
        arch x64
        push rbx
        push r12
        push r13
        mov rbx, rdi
        mov r12, rsi
        mov r13, rdx
    genNodeX64(a, g, root)
    dynasm a:
      arch x64
      test r11, r11
      jnz fwd("jit_z")
      test r10, r10
      jz fwd("jit_z")
      mov eax, 1
      jmp fwd("jit_done")
    a.lab("jit_z")
    dynasm a:
      arch x64
      `xor` eax, eax
    a.lab("jit_done")
    dynasm a:
      arch x64
      pop r13
      pop r12
      pop rbx
      ret()
    a.lab("jit_ovf")
    dynasm a:
      arch x64
      mov qword [r13], 1
      pop r13
      pop r12
      pop rbx
      `xor` eax, eax
      ret()
    var buf = a.finalize()
    result = JitPred(cols: cols, constVals: constVals,
      constNulls: constNulls, nslots: nslots, buf: buf)
    result.fn = cast[JitPredFn](result.buf.ptrAt(AssemblyOffset(0)))

proc tryCompilePred*(whereNode: SqlNode, tbl: DbTable): JitPred =
  ## Analyzes and compiles a WHERE clause; nil when the shape falls
  ## outside the compilable subset. Never raises for subset misses.
  let t = tryBuildPred(whereNode, tbl)
  if not t.ok:
    return nil
  try:
    compilePred(t.cols, t.constVals, t.constNulls, t.root, t.maxDepth)
  except CatchableError:
    nil
