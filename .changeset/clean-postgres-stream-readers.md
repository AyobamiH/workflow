---
'@workflow/world-postgres': patch
---

Clean up Postgres stream reader listeners on EOF, query failure, cancellation, and World shutdown. Closing the streamer now resolves active readers and prevents new readers from attaching after shutdown.
