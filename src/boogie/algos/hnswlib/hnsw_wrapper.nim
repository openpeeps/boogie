# Nim FFI bindings for hnsw_wrapper.{h,cc}

{.passC: "-I.".}
{.compile: "hnsw_wrapper.cc".}

type
  spaceType* {.size: sizeof(cint).} = enum
    l2 = 0, ip = 1, cosine = 2

  hnsw_labeltype* = uint64

  HnswIndex* = object
  PHnswIndex* = ptr HnswIndex

  SearchResult* {.bycopy.} = object
    label*: ptr hnsw_labeltype
    dist*: ptr cfloat
  PSearchResult* = ptr SearchResult

proc newIndex*(space_type: spaceType, dim: cint, max_elements: csize_t,
               M: cint, ef_construction: cint, rand_seed: cint, allow_replace_deleted: cint): PHnswIndex
               {.cdecl, importc.}

proc setEf*(index: PHnswIndex, ef: csize_t) {.cdecl, importc.}
proc indexFileSize*(index: PHnswIndex): csize_t {.cdecl, importc.}
proc saveIndex*(index: PHnswIndex, location: cstring) {.cdecl, importc.}
proc loadIndex*(location: cstring, space_type: spaceType, dim: cint, max_elements: csize_t, allow_replace_deleted: cint): PHnswIndex
               {.cdecl, importc.}

proc addPoints*(index: PHnswIndex, flat_vectors: ptr cfloat, rows: cint, labels: ptr csize_t, num_threads: cint, replace_deleted: cint): cint
               {.cdecl, importc.}

proc markDeleted*(index: PHnswIndex, label: csize_t) {.cdecl, importc.}
proc unmarkDeleted*(index: PHnswIndex, label: csize_t) {.cdecl, importc.}
proc resizeIndex*(index: PHnswIndex, new_size: csize_t) {.cdecl, importc.}
proc getMaxElements*(index: PHnswIndex): csize_t {.cdecl, importc.}
proc getCurrentCount*(index: PHnswIndex): csize_t {.cdecl, importc.}

proc searchKnn*(index: PHnswIndex, flat_vectors: ptr cfloat, rows: cint, k: cint, num_threads: cint): PSearchResult
               {.cdecl, importc.}

proc getAllowReplaceDeleted*(index: PHnswIndex): cint {.cdecl, importc.}
proc getDataByLabel*(index: PHnswIndex, label: csize_t, data: ptr cfloat) {.cdecl, importc.}
proc freeHNSW*(index: PHnswIndex) {.cdecl, importc.}
proc freeResult*(result: PSearchResult) {.cdecl, importc.}

# Helper Nim procs for easier usage
proc searchKnn1*(index: PHnswIndex, query: openArray[float32], k: int, num_threads: int = 1): seq[(hnsw_labeltype, float32)] =
  if query.len == 0 or k <= 0:
    return @[]

  let res = searchKnn(index, unsafeAddr query[0], 1.cint, k.cint, num_threads.cint)
  if res == nil: return @[]

  let labels = cast[ptr UncheckedArray[hnsw_labeltype]](res.label)
  let dists = cast[ptr UncheckedArray[cfloat]](res.dist)

  result = newSeq[(hnsw_labeltype, float32)](k)
  for i in 0..<k:
    result[i] = (labels[i], float32(dists[i]))

  freeResult(res)