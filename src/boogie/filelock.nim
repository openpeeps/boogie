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
## handle dies). The lock is an OS-level `flock(LOCK_EX)` on a sidecar lockfile
## next to the database files, so:
## - two processes opening the SAME path never interleave WAL appends,
##   snapshots, or LSN allocation; the second one blocks (waits) until the
##   first closes instead of failing or corrupting data;
## - a crashing holder releases the lock automatically (the OS drops `flock`
##   with the file description), so there are no stale locks to break;
## - nested opens of the same path inside ONE process share the lock via a
##   process-local refcount (a second `flock` from the same process on another
##   fd could otherwise refuse/ deadlock), preserving the old single-process
##   behavior (e.g. reopen-without-close in tests).
##
## In-memory stores pass no path and get no lock. On non-POSIX platforms the
## lock is a no-op (returns a zero handle).

import std/[tables, locks, os]

when defined(posix):
  proc c_flock(fd: cint, op: cint): cint {.importc: "flock",
    header: "<sys/file.h>".}
  proc c_fcntl(fd: cint, cmd: cint, arg: cint): cint {.importc: "fcntl",
    header: "<fcntl.h>".}
  const
    FlockEx = 2.cint
    FSetFd = 2.cint
    FdCloExec = 1.cint

type
  FileLockError* = object of CatchableError

  LockEntry = ref object
    ## One OS lock per canonical path per process. Shared by every `FileLock`
    ## handle for that path; closed when the last handle dies.
    path: string
    fh: File
    refs: int

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
    if flHolds.hasKey(path) and cast[pointer](flHolds[path]) == self:
      dec flHolds[path].refs
      if flHolds[path].refs <= 0:
        fh = flHolds[path].fh
        flHolds.del(path)
        done = true
    release(flMu)
    if done:
      fh.close()

proc acquireFileLock*(lockPath: string): FileLock =
  ## Blocks until the exclusive lock for `lockPath` is held, then returns a
  ## handle. Same-process re-entry shares the held lock (refcounted).
  ## `lockPath` itself is created if missing (its parent dir must exist).
  ## Returns a zero (nil-entry) handle on non-POSIX platforms.
  when not defined(posix):
    discard lockPath
    return FileLock()
  else:
    let canon = canonical(lockPath)
    acquire(flMu)
    if flHolds.hasKey(canon):
      let e = flHolds[canon]
      inc e.refs
      release(flMu)
      return FileLock(entry: e)
    release(flMu)
    # Slow path: create/open the lockfile, then block in flock.
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
    if c_flock(cint(getFileHandle(fh)), FlockEx) != 0:
      fh.close()
      raise newException(FileLockError, "cannot lock: " & canon)
    let e = LockEntry(path: canon, fh: fh, refs: 1)
    acquire(flMu)
    # A concurrent same-process acquire may have won while we blocked: fold
    # into it instead of holding two descriptions.
    if flHolds.hasKey(canon):
      let prev = flHolds[canon]
      inc prev.refs
      release(flMu)
      fh.close()
      return FileLock(entry: prev)
    flHolds[canon] = e
    release(flMu)
    FileLock(entry: e)
