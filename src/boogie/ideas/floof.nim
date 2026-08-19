import std/[algorithm, sequtils, strutils, bitops, os, cpuinfo]
import nimsimd/[sse2]

when (defined(gcc) or defined(clang)) and not defined(noSIMD):
  {.passC: "-msse2".}

type
  SearchResult* = object
    text*: string
    score*: float32

  ThreadData = object
    query: string
    haystack: seq[string]
    startIdx, endIdx: int
    results: ptr seq[SearchResult]

const VectorSize = 16 # SSE2 register size in bytes

# SSE2 helper functions
func toLowerSimd(ch: M128i): M128i {.inline.} =
  # 'A' is 0x41, 'Z' is 0x5A
  let upperTest = mm_and_si128(
    mm_cmpgt_epi8(mm_set1_epi8(0x5B), ch), mm_cmpgt_epi8(ch, mm_set1_epi8(0x40))
  )
  let toLowerAdd = mm_and_si128(upperTest, mm_set1_epi8(0x20))
  result = mm_add_epi8(ch, toLowerAdd)

func findNextMatch(
    pattern: char, text: string, startPos: int
): tuple[pos: int, isWordStart: bool] =
  if startPos >= text.len:
    return (pos: -1, isWordStart: false)

  let patternVec = mm_set1_epi8(pattern.int8)
  var pos = startPos

  while pos <= text.len - VectorSize:
    # Load text chunk and convert both pattern and text to lowercase
    let textVec = toLowerSimd(mm_loadu_si128(cast[ptr M128i](unsafeAddr text[pos])))
    let patternLowerVec = toLowerSimd(patternVec)

    # Find matches
    let matches = mm_cmpeq_epi8(textVec, patternLowerVec)
    let matchMask = mm_movemask_epi8(matches)

    if matchMask != 0:
      let offset = countTrailingZeroBits(uint16(matchMask))
      let matchPos = pos + offset

      # Check if it's a word start
      var isWordStart = false
      if matchPos == 0:
        isWordStart = true
      elif matchPos > 0:
        isWordStart = text[matchPos - 1] == ' '

      return (pos: matchPos, isWordStart: isWordStart)

    pos += VectorSize

  # Handle remaining bytes
  if pos < text.len:
    var lastChunk: array[VectorSize, char]
    let remaining = text.len - pos
    zeroMem(addr lastChunk[0], VectorSize)
    copyMem(addr lastChunk[0], unsafeAddr text[pos], remaining)

    let textVec = toLowerSimd(mm_loadu_si128(cast[ptr M128i](addr lastChunk[0])))
    let patternLowerVec = toLowerSimd(patternVec)
    let matches = mm_cmpeq_epi8(textVec, patternLowerVec)
    let matchMask = mm_movemask_epi8(matches) and ((1 shl remaining) - 1)

    if matchMask != 0:
      let offset = countTrailingZeroBits(uint16(matchMask))
      let matchPos = pos + offset
      if matchPos < text.len:
        var isWordStart = false
        if matchPos == 0:
          isWordStart = true
        elif matchPos > 0:
          isWordStart = text[matchPos - 1] == ' '

        return (pos: matchPos, isWordStart: isWordStart)

  return (pos: -1, isWordStart: false)

func scoreMatchSSE2*(query, text: string): float32 =
  if query.len == 0 or text.len == 0:
    return 0.0

  var
    score = 0.0'f32
    lastMatchPos = -1
    searchPos = 0
    consecutiveMatches = 0

  for qChar in query:
    let matchResult = findNextMatch(qChar, text, searchPos)
    if matchResult.pos == -1:
      return 0.0

    score += 1.0
    if matchResult.isWordStart:
      score += 2.0

    if lastMatchPos != -1 and matchResult.pos == lastMatchPos + 1:
      consecutiveMatches += 1
      score += float32(consecutiveMatches) * 0.5
    else:
      consecutiveMatches = 0

    # Case matching bonus using SIMD
    let queryVec = mm_set1_epi8(qChar.int8)
    let textVec = mm_loadu_si128(cast[ptr M128i](unsafeAddr text[matchResult.pos]))
    let caseMatch = mm_cmpeq_epi8(queryVec, textVec)
    let caseMask = mm_movemask_epi8(caseMatch)
    if (caseMask and 1) != 0:
      score += 0.5

    lastMatchPos = matchResult.pos
    searchPos = matchResult.pos + 1

  result = score / float32(max(text.len, query.len))
  if lastMatchPos < text.len div 2:
    result *= 1.2

proc searchThread(data: ThreadData) {.thread.} =
  for i in data.startIdx ..< data.endIdx:
    let score = scoreMatchSSE2(data.query, data.haystack[i])
    if score > 0:
      data.results[].add(SearchResult(text: data.haystack[i], score: score))

proc search*(query: string, haystack: seq[string]): seq[SearchResult] =
  runnableExamples:
    import floof
    import std/[sequtils, strutils]
    let
      dictionary = toSeq(walkDir("/usr/share/applications/")).mapIt(
        it.path.splitPath().tail.replace(".desktop", "")
      )
      searchTerm = paramStr(1)
  
    echo "Searching for: ", searchTerm
    let results = search(searchTerm, dictionary)
    for res in results:
      echo res.text, " (score: ", res.score.formatFloat(ffDecimal, 3), ")"
  
  if haystack.len == 0 or query.len == 0:
    return @[]

  let
    numThreads = countProcessors()
    chunkSize = max(256, haystack.len div numThreads)

  var
    threads: seq[Thread[ThreadData]]
    threadResults: seq[ptr seq[SearchResult]]

  threads.setLen(numThreads)
  threadResults.setLen(numThreads)

  for i in 0 ..< numThreads:
    threadResults[i] = create(seq[SearchResult])
    threadResults[i][] = @[]

  var startIdx = 0
  for i in 0 ..< numThreads:
    let endIdx = min(startIdx + chunkSize, haystack.len)
    if startIdx >= endIdx:
      break

    let threadData = ThreadData(
      query: query,
      haystack: haystack,
      startIdx: startIdx,
      endIdx: endIdx,
      results: threadResults[i],
    )

    createThread(threads[i], searchThread, threadData)
    startIdx += chunkSize

  for i in 0 ..< numThreads:
    if startIdx > i * chunkSize:
      joinThread(threads[i])

  result = @[]
  for i in 0 ..< numThreads:
    if startIdx > i * chunkSize:
      result.add(threadResults[i][])
      dealloc(threadResults[i])

  result.sort(
    proc(x, y: SearchResult): int =
      result = cmp(y.score, x.score)
      if result == 0:
        result = cmp(x.text.len, y.text.len)
      if result == 0:
        result = cmp(x.text, y.text)
  )