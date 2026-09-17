# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

## Cross-process file locking for Boogie disk stores.
##
## Every disk-backed store holds a `FileLock` for its whole lifetime, acquired
## BEFORE the WAL/snapshot is opened and released on `close` (or when the last
## handle dies). The lock is an OS-level `flock` on a sidecar lockfile
## next to the database files:
## - writers hold `LOCK_EX`; readers opened with `readOnly = true` hold
##   `LOCK_SH`, so any number of concurrent reader processes can open the
##   SAME path at once while a writer still excludes everyone;
## - two writers opening the SAME path never interleave WAL appends,
##   snapshots, or LSN allocation; the second one blocks (waits) until the
##   first closes instead of failing or corrupting data (or raises
##   `FileLockBusyError` when opened non-blocking / with a timeout);
## - a crashing holder releases the lock automatically (the OS drops `flock`
##   with the file description), so there are no stale locks to break;
## - nested opens of the same path inside ONE process share the lock via a
##   process-local refcount (a second `flock` from the same process on another
##   fd could otherwise refuse/ deadlock), preserving the old single-process
##   behavior (e.g. reopen-without-close in tests).
##
## In-memory stores pass no path and get no lock. On non-POSIX platforms the
## lock is a no-op (returns a zero handle).

import std/[tables, locks, os, times, monotimes]

when defined(posix):
  proc c_flock(fd: cint, op: cint): cint {.importc: "flock",
    header: "<sys/file.h>".}
  proc c_fcntl(fd: cint, cmd: cint, arg: cint): cint {.importc: "fcntl",
    header: "<fcntl.h>".}
  const
    FlockSh = 1.cint
    FlockEx = 2.cint
    FlockNb = 4.cint
    FSetFd = 2.cint
    FdCloExec = 1.cint

type
  FileLockError* = object of CatchableError
  FileLockBusyError* = object of FileLockError
    ## Raised when a lock cannot be acquired without blocking (non-blocking
    ## mode) or within the given timeout. Callers can retry or surface a
    ## "database is locked" error instead of hanging forever.

  LockMode* = enum
    ## `lmShared` allows concurrent holders (read-only opens).
    ## `lmExclusive` excludes every other holder (read-write opens).
    lmShared, lmExclusive

  LockEntry = ref object
    ## One OS lock per canonical path per process. Shared by every `FileLock`
    ## handle for that path; closed when the last handle dies.
    path: string
    fh: File
    refs: int
    mode: LockMode

  FileLock* = object
    ## Handle to a held lock. A zero `FileLock` holds nothing. Copying shares
    ## the held lock (the OS lock is released when the last handle detaches);
    ## release explicitly by assigning `FileLock()` (e.g. in a store `close`).
    entry: LockEntry

var
  flMu: Lock
  flHolds: Table[string, LockEntry]

initLock(flMu)

proc canonical(p: string): string =
  normalizedPath(absolutePath(p))

# NOTE: lifecycle hooks must precede the first construction/destruction of the
# type in this module, otherwise the compiler binds an implicit hook instead.
proc `=destroy`*(l: var FileLock) =
  when defined(posix):
    if l.entry.isNil:
      return
    # Each handle detaches once. The entry may already be gone (fully released
    # earlier) or replaced (released then re-acquired by someone else): only
    # detach when the table still holds OUR entry.
    let path = l.entry.path
    let self = cast[pointer](l.entry)
    var fh: File
    var done = false
    acquire(flMu)
    try:
      if flHolds.hasKey(path) and cast[pointer](flHolds[path]) == self:
        dec flHolds[path].refs
        if flHolds[path].refs <= 0:
          fh = flHolds[path].fh
          flHolds.del(path)
          done = true
    finally:
      release(flMu)
    if done:
      fh.close()

proc flockOp(mode: LockMode, nonblock: bool): cint =
  when defined(posix):
    let base =
      case mode
      of lmShared: FlockSh
      of lmExclusive: FlockEx
    if nonblock: base or FlockNb else: base
  else:
    0.cint

proc lockBusy(lockPath: string): ref FileLockBusyError =
  result = newException(FileLockBusyError, "database is locked: " & lockPath)

proc acquireFileLock*(lockPath: string, mode: LockMode = lmExclusive,
    blocking = true, timeoutMs = -1): FileLock =
  ## Acquires the lock for `lockPath` and returns a handle.
  ##
  ## - `mode`: `lmExclusive` (default, writers) excludes everyone;
  ##   `lmShared` (read-only opens) allows concurrent shared holders but is
  ##   excluded by a writer. Many processes may hold `lmShared` at once, which
  ##   is what lets concurrent readers open the same `.db` path without
  ##   hanging.
  ## - `blocking = true` (default) waits until the lock is held, preserving
  ##   the historical behavior. `blocking = false` tries once and raises
  ##   `FileLockBusyError` instead of waiting.
  ## - `timeoutMs >= 0` bounds a blocking wait: polls with `LOCK_NB` and raises
  ##   `FileLockBusyError` after the timeout expires. `-1` waits indefinitely.
  ##
  ## Same-process re-entry shares the held lock (refcounted). An exclusive
  ## holder already implies shared access, so requesting `lmShared` while this
  ## process holds `lmExclusive` just shares the handle. Requesting
  ## `lmExclusive` while this process only holds `lmShared` raises
  ## `FileLockError` (a shared-to-exclusive upgrade cannot be done safely;
  ## close the read-only handle and reopen read-write instead).
  ## `lockPath` itself is created if missing (its parent dir must exist).
  ## Returns a zero (nil-entry) handle on non-POSIX platforms.
  when not defined(posix):
    discard lockPath
    discard mode
    discard blocking
    discard timeoutMs
    return FileLock()
  else:
    let canon = canonical(lockPath)
    acquire(flMu)
    try:
      if flHolds.hasKey(canon):
        let e = flHolds[canon]
        if e.mode == lmExclusive or e.mode == mode:
          inc e.refs
          return FileLock(entry: e)
        raise newException(FileLockError,
          "cannot upgrade shared lock to exclusive for: " & canon &
          " (close the read-only handle first)")
    finally:
      release(flMu)
    # Slow path: create/open the lockfile, then lock it.
    # The content is irrelevant (presence is the lock); pre-creating keeps us
    # independent of the exact create-semantics of fmReadWrite.
    if not fileExists(canon):
      try:
        writeFile(canon, "")
      except OSError:
        discard
    var fh: File
    if not open(fh, canon, fmReadWrite):
      raise newException(FileLockError, "cannot open lockfile: " & canon)
    # Do not leak the fd across fork+exec (a spawned child must not pin our
    # lock after we release it); best-effort, the lock works without it.
    discard c_fcntl(cint(getFileHandle(fh)), FSetFd, FdCloExec)
    let fd = cint(getFileHandle(fh))
    if not blocking or timeoutMs == 0:
      if c_flock(fd, flockOp(mode, true)) != 0:
        fh.close()
        raise lockBusy(canon)
    elif timeoutMs < 0:
      if c_flock(fd, flockOp(mode, false)) != 0:
        fh.close()
        raise newException(FileLockError, "cannot lock: " & canon)
    else:
      let deadline = getMonoTime() + initDuration(milliseconds = timeoutMs)
      while true:
        if c_flock(fd, flockOp(mode, true)) == 0:
          break
        if getMonoTime() >= deadline:
          fh.close()
          raise lockBusy(canon)
        sleep(10)
    let e = LockEntry(path: canon, fh: fh, refs: 1, mode: mode)
    acquire(flMu)
    try:
      # A concurrent same-process acquire may have won while we blocked: fold
      # into it instead of holding two descriptions.
      if flHolds.hasKey(canon):
        let prev = flHolds[canon]
        if prev.mode == lmExclusive or prev.mode == mode:
          inc prev.refs
          fh.close()
          return FileLock(entry: prev)
        # Same-process mode conflict (shared held, exclusive requested):
        # drop the just-acquired OS lock and fail loudly so the caller
        # notices the SH->EX misuse instead of silently replacing the entry.
        fh.close()
        raise newException(FileLockError,
          "cannot upgrade shared lock to exclusive for: " & canon &
          " (close the read-only handle first)")
      flHolds[canon] = e
    finally:
      release(flMu)
    FileLock(entry: e)

proc tryAcquireFileLock*(lockPath: string,
    mode: LockMode = lmExclusive): FileLock =
  ## Non-blocking variant of `acquireFileLock`: tries once and raises
  ## `FileLockBusyError` when another process holds a conflicting lock.
  acquireFileLock(lockPath, mode, blocking = false)
