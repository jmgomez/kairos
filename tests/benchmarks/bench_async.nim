import ../../kairos
import chronos

proc onRequest(req: Request): Future[void] {.gcsafe, async.} =
  # Simulate a 10ms DB query
  await sleepAsync(10.milliseconds)
  req.send(Http200, "Hello after async work!")

echo "Kairos async benchmark server starting on port 9080..."
echo "Run: wrk -t4 -c100 -d10s http://127.0.0.1:9080/"
run(onRequest, initSettings(port = Port(9080), numThreads = 0))
