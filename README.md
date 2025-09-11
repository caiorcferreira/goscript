

# Fundamental rule of concurrency

Only routines that write to a `Pipe.Out()` may call `Close()` on the Pipe.