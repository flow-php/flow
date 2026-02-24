Stdout is a special type of filesystem allowing to 
write straight to stdout of the process.

`Stdout is a write-only filesystem. It is not possible to read from it.`

Its main purpose is to allow web servers to stream data to the client without buffering it in memory.