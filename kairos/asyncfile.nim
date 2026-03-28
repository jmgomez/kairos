## Async file I/O for chronos — port of std/asyncfile using chronos primitives.
## POSIX: non-blocking fd + addReader2/removeReader2
## Windows: OVERLAPPED I/O + IOCP via chronos CustomOverlapped

import chronos

when defined(windows):
  import std/[os, winlean]
  from chronos/osdefs import nil
  import chronos/internal/asyncengine

  const
    ERROR_HANDLE_EOF = 38'i32
    ERROR_IO_PENDING = 997'i32

  type
    AsyncFile* = ref object
      fd: AsyncFD
      offset: int64

  proc getDesiredAccess(mode: FileMode): int32 =
    case mode
    of fmRead: GENERIC_READ
    of fmWrite, fmAppend: GENERIC_WRITE
    of fmReadWrite, fmReadWriteExisting: GENERIC_READ or GENERIC_WRITE

  proc getCreationDisposition(mode: FileMode, filename: string): int32 =
    case mode
    of fmRead, fmReadWriteExisting: OPEN_EXISTING.int32
    of fmReadWrite: OPEN_ALWAYS.int32
    of fmWrite, fmAppend:
      if fileExists(filename): OPEN_EXISTING.int32
      else: CREATE_NEW.int32

  proc getAsyncFileSize*(f: AsyncFile): int64 =
    var size: int64
    proc getFileSizeEx(hFile: Handle, lpFileSize: ptr int64): WINBOOL
      {.importc: "GetFileSizeEx", stdcall, dynlib: "kernel32".}
    if getFileSizeEx(winlean.Handle(f.fd), addr size) == 0:
      raiseOSError(osLastError())
    size

  proc openAsync*(filename: string, mode = fmRead): AsyncFile =
    let flags = int32(0x40000000) or int32(0x80)  # FILE_FLAG_OVERLAPPED | FILE_ATTRIBUTE_NORMAL
    let fd = createFileW(
      newWideCString(filename),
      getDesiredAccess(mode),
      FILE_SHARE_READ,
      nil,
      getCreationDisposition(mode, filename),
      flags,
      0
    )
    if fd == INVALID_HANDLE_VALUE:
      raiseOSError(osLastError())
    result = AsyncFile(fd: AsyncFD(fd), offset: 0)
    register2(result.fd).tryGet()
    if mode == fmAppend:
      result.offset = getAsyncFileSize(result)

  proc read*(f: AsyncFile, size: int): Future[string] {.async.} =
    var retFuture = newFuture[string]("asyncfile.read")
    var buffer = alloc0(size)
    var ovl = RefCustomOverlapped(data: CompletionData(
      cb: proc(udata: pointer) {.gcsafe, raises: [].} =
        let ovl = cast[RefCustomOverlapped](udata)
        if not retFuture.finished:
          if ovl.data.errCode == OSErrorCode(0) or ovl.data.errCode == OSErrorCode(-1):
            let count = ovl.data.bytesCount
            if count > 0:
              var data = newString(count)
              copyMem(addr data[0], buffer, count)
              f.offset.inc count.int64
              retFuture.complete(data)
            else:
              retFuture.complete("")
          else:
            if ovl.data.errCode.int32 == ERROR_HANDLE_EOF.int32:
              retFuture.complete("")
            else:
              retFuture.fail(newOSError(ovl.data.errCode))
        if buffer != nil:
          dealloc buffer
          buffer = nil
    ))
    ovl.offset = uint32(f.offset and 0xffffffff)
    ovl.offsetHigh = uint32(f.offset shr 32)
    let ret = winlean.readFile(
      winlean.Handle(f.fd), buffer, size.int32, nil,
      cast[winlean.POVERLAPPED](addr ovl[])
    )
    if ret == 0:
      let err = osLastError()
      if err.int32 != ERROR_IO_PENDING.int32:
        if buffer != nil:
          dealloc buffer
          buffer = nil
        if err.int32 == ERROR_HANDLE_EOF.int32:
          retFuture.complete("")
        else:
          retFuture.fail(newOSError(err))
    return await retFuture

  proc readAll*(f: AsyncFile): Future[string] {.async.} =
    result = ""
    while true:
      let chunk = await f.read(4096)
      if chunk.len == 0: break
      result.add(chunk)

  proc write*(f: AsyncFile, data: string): Future[void] {.async.} =
    var retFuture = newFuture[void]("asyncfile.write")
    var ovl = RefCustomOverlapped(data: CompletionData(
      cb: proc(udata: pointer) {.gcsafe, raises: [].} =
        let ovl = cast[RefCustomOverlapped](udata)
        if not retFuture.finished:
          if ovl.data.errCode == OSErrorCode(0) or ovl.data.errCode == OSErrorCode(-1):
            f.offset.inc ovl.data.bytesCount.int64
            retFuture.complete()
          else:
            retFuture.fail(newOSError(ovl.data.errCode))
    ))
    ovl.offset = uint32(f.offset and 0xffffffff)
    ovl.offsetHigh = uint32(f.offset shr 32)
    let ret = winlean.writeFile(
      winlean.Handle(f.fd), unsafeAddr data[0], data.len.int32, nil,
      cast[winlean.POVERLAPPED](addr ovl[])
    )
    if ret == 0:
      let err = osLastError()
      if err.int32 != ERROR_IO_PENDING.int32:
        retFuture.fail(newOSError(err))
    await retFuture

  proc close*(f: AsyncFile) =
    discard winlean.closeHandle(winlean.Handle(f.fd))


else:
  # POSIX
  import std/[os, posix]

  type
    AsyncFile* = ref object
      fd: AsyncFD
      offset: int64

  proc openAsync*(filename: string, mode = fmRead): AsyncFile =
    let flags = case mode
      of fmRead: O_RDONLY
      of fmWrite: O_WRONLY or O_CREAT or O_TRUNC
      of fmReadWrite: O_RDWR or O_CREAT
      of fmReadWriteExisting: O_RDWR
      of fmAppend: O_WRONLY or O_CREAT or O_APPEND
    let perm = S_IRUSR or S_IWUSR or S_IRGRP or S_IROTH
    let fd = open(filename.cstring, flags.cint, perm)
    if fd == -1:
      raiseOSError(osLastError())
    var curFlags = fcntl(fd, F_GETFL)
    if curFlags == -1:
      raiseOSError(osLastError())
    if fcntl(fd, F_SETFL, curFlags or O_NONBLOCK) == -1:
      raiseOSError(osLastError())
    result = AsyncFile(fd: fd.AsyncFD, offset: 0)
    register2(result.fd).tryGet()

  proc read*(f: AsyncFile, size: int): Future[string] {.async.} =
    var buf = newString(size)
    let res = posix.read(f.fd.cint, addr buf[0], size.cint)
    if res > 0:
      buf.setLen(res)
      f.offset.inc(res)
      return buf
    elif res == 0:
      return ""
    else:
      let err = osLastError()
      if err.int32 == EAGAIN:
        var retFuture = newFuture[string]("asyncfile.read")
        proc cb(udata: pointer) {.gcsafe, raises: [].} =
          discard removeReader2(f.fd)
          let res2 = posix.read(f.fd.cint, addr buf[0], size.cint)
          if res2 >= 0:
            buf.setLen(res2)
            f.offset.inc(res2)
            retFuture.complete(buf)
          else:
            retFuture.fail(newOSError(osLastError()))
        addReader2(f.fd, cb).tryGet()
        return await retFuture
      else:
        raiseOSError(err)

  proc readAll*(f: AsyncFile): Future[string] {.async.} =
    result = ""
    while true:
      let chunk = await f.read(4096)
      if chunk.len == 0: break
      result.add(chunk)

  proc write*(f: AsyncFile, data: string): Future[void] {.async.} =
    var written = 0
    while written < data.len:
      let res = posix.write(f.fd.cint, unsafeAddr data[written], (data.len - written).cint)
      if res > 0:
        written += res
        f.offset.inc(res)
      elif res == -1:
        let err = osLastError()
        if err.int32 == EAGAIN:
          var retFuture = newFuture[void]("asyncfile.write")
          proc cb(udata: pointer) {.gcsafe, raises: [].} =
            discard removeWriter2(f.fd)
            retFuture.complete()
          addWriter2(f.fd, cb).tryGet()
          await retFuture
        else:
          raiseOSError(err)

  proc close*(f: AsyncFile) =
    discard unregister2(f.fd)
    discard posix.close(f.fd.cint)

  proc getFileSize*(f: AsyncFile): int64 =
    let cur = lseek(f.fd.cint, 0, SEEK_CUR)
    result = lseek(f.fd.cint, 0, SEEK_END)
    discard lseek(f.fd.cint, cur, SEEK_SET)
