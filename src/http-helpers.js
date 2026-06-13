// Shared HTTP plumbing for the worker route modules.

// Wrap an async Express handler so a rejected promise is forwarded to Express's
// error pipeline via next(err) instead of becoming an unhandled promise
// rejection — which, under Node's default, crashes the entire worker process
// (taking every live SSE connection on that worker down with it).
export function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

// Tail error-handling middleware. A failed request (e.g. the DB pool is
// momentarily exhausted) degrades to a 503 for THAT request only; the worker
// and everyone else's connections keep running. Mounted last on each router.
export function errorHandler(err, req, res, _next) {
  console.error(`[${req.method} ${req.originalUrl}] handler error:`, err?.message ?? err);
  // If we've already started writing the response (e.g. an SSE stream), we can't
  // swap in a JSON error — just drop the connection.
  if (res.headersSent) return res.end();
  res.status(503).json({ error: 'service unavailable' });
}