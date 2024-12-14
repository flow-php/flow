Flow is based on a dedicated file system library that provides a clear 
separation between reading/writing streams.  

Filesystem component is available as a standalone package; 

```
composer require flow-php/filesystem
```

It was inspired by a linux FStab, and similarly to it to start
using a filesystem it needs to be first mounted. 
By default `fstab()` function registers two default filesystems: 

- local native filesystem 
- stdout write-only filesystem

All filesystems supports listing files and opening streams
into which you can read or write data.

## Listing Files
---

To list files use `Filesystem::list(Path $path)` method
which also supports `glob` pattern.

List method returns and generator of `FileStatus` objects.

Alternatively to check if a single file exists, use `Filesystem::status(Path $path) : ?FileStatus`

## Reading Data
---

Reading data from a filesystem is done by opening a stream
and deciding if you want to read it at once or in chunks.

Additionally, you can read a part of a file from a specific offset.

## Writing Data
---

Writing data to a filesystem is done by opening a stream throught
one of two methods: 

- `appendTo(Path $path) : DestinationStream`
- `writeTo(Path $path) : DestinationStream`

