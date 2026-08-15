# Enable threads + ARC for the concurrency tests (test7+). The stores'
# enableConcurrency paths require --threads:on (compile-time assert).
switch("threads", "on")
switch("mm", "arc")
