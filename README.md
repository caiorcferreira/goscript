

# Fundamental rule of concurrency

Only routines that write to a `Pipe.Out()` may call `Close()` on the Pipe.

# Inspiration
- https://github.com/bitfield/script
- https://github.com/redpanda-data/connect