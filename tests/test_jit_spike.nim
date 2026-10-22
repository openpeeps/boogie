import std/[unittest, times]
import pkg/dynasm
import ../src/boogie/sqlengine
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# dynasm integration spike for the SQL JIT (plan Phase 5).
#
# Proves that dynasm assembles and runs inside boogie's toolchain and
# establishes the kernel shape a future predicate JIT will use: a native
# function over a flat data pointer plus scalar bounds. No SQL semantics
# yet; the SQL VM remains the only execution path.
#
# Kernel: count of int64 elements strictly greater than a threshold.
# Signature (cdecl): countGt(data: pointer, n: int64, threshold: int64)
# SysV x64: RDI, RSI, RDX. Win64: RCX, RDX, R8. AArch64: X0, X1, X2.
# ---------------------------------------------------------------------------

type CountGtFn = proc(data: pointer, n: int64, threshold: int64): int64 {.cdecl.}

proc buildCountGt*(): ExecutableBuffer =
  when defined(arm64) or defined(aarch64):
    var a = newAssembler(akAarch64)
    dynasm a:
      arch aarch64
      alias data, x0
      alias n, x1
      alias threshold, x2
      mov x9, xzr # count
      mov x10, xzr # i
      label loop
      cmp x10, n
      b.ge fwd("done")
      ldr x11, [data, x10, lsl, 3]
      cmp x11, threshold
      b.le fwd("next")
      add x9, x9, 1
      label next
      add x10, x10, 1
      b bwd("loop")
      label done
      mov x0, x9
      ret()
    a.finalize()
  else:
    var a = newAssembler(akX64)
    dynasm a:
      arch x64
      alias data, rdi, rcx
      alias n, rsi, rdx
      alias threshold, rdx, r8
      `xor` r10, r10 # count (r10: scratch on both ABIs)
      `xor` r11d, r11d # i (r11: scratch on both ABIs)
      label loop
      cmp r11, n
      jge fwd("done")
      mov rax, [data + r11*8]
      cmp rax, threshold
      jle fwd("next")
      add r10, 1
      label next
      add r11, 1
      jmp bwd("loop")
      label done
      mov rax, r10
      ret()
    a.finalize()

proc countGtRef(data: openArray[int64], threshold: int64): int64 =
  for v in data:
    if v > threshold: inc result

suite "dynasm spike (SQL JIT toolchain)":
  test "predicate-count kernel matches reference":
    const N = 10_000
    var xs = newSeq[int64](N)
    for i in 0..<N:
      xs[i] = int64((i * 37 + 11) mod 1000)
    var buf = buildCountGt()
    check buf.len > 0
    let countGt = cast[CountGtFn](buf.ptrAt(AssemblyOffset(0)))
    for threshold in [0'i64, 499'i64, 998'i64, 1000'i64]:
      check countGt(cast[pointer](addr xs[0]), int64(N), threshold) ==
        countGtRef(xs, threshold)
    buf.destroy()

  test "predicate-count kernel throughput":
    const N = 100_000
    var xs = newSeq[int64](N)
    for i in 0..<N:
      xs[i] = int64(i)
    var buf = buildCountGt()
    let countGt = cast[CountGtFn](buf.ptrAt(AssemblyOffset(0)))
    let iters = 200
    var acc = 0'i64
    let t0 = cpuTime()
    for _ in 0..<iters:
      acc += countGt(cast[pointer](addr xs[0]), int64(N), 50_000'i64)
    let secs = cpuTime() - t0
    check acc == 49_999'i64 * iters
    echo "[bench][jit-spike] countGt=", float(N * iters) / secs, " elems/s"
    buf.destroy()

suite "hot-loop detection seam":
  test "onHotLoop fires once per VM scan-loop site":
    var eng = newSqlEngine(newInMemoryStore())
    var fired = 0
    eng.onHotLoop(proc(chunk: Chunk, pcIdx: int) = inc fired)
    discard eng.execSql("CREATE TABLE t (id INTEGER PRIMARY KEY, n INT)")
    for i in 1..200:
      discard eng.execSql("INSERT INTO t (n) VALUES (?)", [initValue(int64(i))])
    let res = eng.execSql("SELECT id FROM t WHERE n > 5; SELECT id FROM t WHERE n > 6")
    check res.rows.len == 194
    check fired == 2
    # Both sites already fired; reruns stay quiet (hook fires once per site).
    discard eng.execSql("SELECT id FROM t WHERE n > 5; SELECT id FROM t WHERE n > 6")
    check fired == 2

suite "direct kernel calls":
  test "compiled n>5 kernel answers directly":
    var eng = newSqlEngine(newInMemoryStore())
    discard eng.execSql("CREATE TABLE t (id INTEGER PRIMARY KEY, n INT)")
    discard eng.execSql("INSERT INTO t (n) VALUES (1)")
    discard eng.execSql("SELECT id FROM t WHERE id < 0; SELECT id FROM t WHERE n > 5")
    check eng.jitPredCount() == 2
    let pred = eng.jitPreds[1]
    check pred.cols == @["n"]
    check pred.constVals == @[5'i64]
    echo "[jit-direct] nslots=", pred.nslots, " cols=", pred.cols,
      " consts=", pred.constVals
    var slots = newSeq[int64](pred.nslots)
    var nulls = newSeq[int64](pred.nslots)
    var err: int64 = 0
    proc call(v, c: int64, vn, cn: int64): int64 =
      slots[0] = v
      nulls[0] = vn
      slots[1] = c
      nulls[1] = cn
      err = 0
      pred.fn(addr slots[0], addr nulls[0], addr err)
    check call(7, 5, 0, 0) == 1
    check err == 0
    check call(3, 5, 0, 0) == 0
    check call(5, 5, 0, 0) == 0
    check call(7, 5, 1, 0) == 0
    check call(7, 5, 0, 1) == 0
