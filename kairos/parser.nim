## Minimal HTTP request parser — inlined from httpx/parser.nim
## (MIT License, Copyright (c) 2020 Dominik Picheta / Zeshen Xing)

import std/options
import httpcore

func parseHttpMethod*(data: string): Option[HttpMethod] =
  if data.len < 3: return none(HttpMethod)
  case data[0]
  of 'G':
    if data[1] == 'E' and data[2] == 'T': return some(HttpGet)
  of 'H':
    if data.len >= 4 and data[1] == 'E' and data[2] == 'A' and data[3] == 'D':
      return some(HttpHead)
  of 'P':
    if data.len >= 4 and data[1] == 'O' and data[2] == 'S' and data[3] == 'T':
      return some(HttpPost)
    if data[1] == 'U' and data[2] == 'T': return some(HttpPut)
    if data.len >= 5 and data[1] == 'A' and data[2] == 'T' and
       data[3] == 'C' and data[4] == 'H':
      return some(HttpPatch)
  of 'D':
    if data.len >= 6 and data[1] == 'E' and data[2] == 'L' and
       data[3] == 'E' and data[4] == 'T' and data[5] == 'E':
      return some(HttpDelete)
  of 'O':
    if data.len >= 7 and data[1] == 'P' and data[2] == 'T' and
       data[3] == 'I' and data[4] == 'O' and data[5] == 'N' and data[6] == 'S':
      return some(HttpOptions)
  else: discard
  none(HttpMethod)

func parsePathRange*(data: string): (int, int) {.inline.} =
  ## Returns (start, end) indices of the path in the raw header data.
  ## Returns (-1, -1) if not found. No allocation.
  if data.len == 0: return (-1, -1)
  var i = 2
  while i < data.len and data[i] notin {' ', '\0'}: inc i
  if i < data.len and data[i] == ' ':
    inc i
    let start = i
    while i < data.len and data[i] notin {' ', '\0'}: inc i
    if i < data.len and data[i] == ' ':
      return (start, i)
  (-1, -1)

func parsePath*(data: string): Option[string] =
  ## Returns a copy of the path. Use parsePathRange for zero-alloc access.
  let (s, e) = parsePathRange(data)
  if s >= 0: some(data[s ..< e])
  else: none(string)

func parseHeaders*(data: string): Option[HttpHeaders] =
  if data.len == 0: return none(HttpHeaders)
  var pairs: seq[(string, string)] = @[]
  var i = 0
  while i < data.len and data[i] != '\l': inc i
  if i >= data.len: return none(HttpHeaders)
  inc i
  var value = false
  var current: (string, string) = ("", "")
  while i < data.len:
    case data[i]
    of ':':
      if value: current[1].add(':')
      value = true
    of ' ':
      if value:
        if current[1].len != 0: current[1].add(data[i])
      else:
        current[0].add(data[i])
    of '\c': discard
    of '\l':
      if current[0].len == 0:
        return some(newHttpHeaders(pairs))
      pairs.add(current)
      value = false
      current = ("", "")
    else:
      if value: current[1].add(data[i])
      else: current[0].add(data[i])
    inc i
  none(HttpHeaders)
