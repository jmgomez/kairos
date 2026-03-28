import ../../kairos

proc onRequest(req: Request): Future[void] {.gcsafe, async.} =
  req.send(Http200, "Hello, World!")

echo "Kairos benchmark (1 thread) on port 9080..."
echo "Run: wrk -t4 -c100 -d10s http://127.0.0.1:9080/"
run(onRequest, initSettings(port = Port(9080), numThreads = 1))
