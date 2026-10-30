<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Generator;
use SensitiveParameter;

interface Filesystem
{
    /**
     * @param array<array-key, mixed> $configuration untrusted: decoded from another process, validated here
     *
     * @throws InvalidArgumentException on a configuration this class cannot rebuild from
     */
    public static function fromArray(#[SensitiveParameter] array $configuration): static;

    public function appendTo(Path $path): DestinationStream;

    /**
     * Bytes become visible whole at close().
     *
     * @throws FileExistsException from DestinationStream::close() when the path exists at that moment
     */
    public function create(Path $path): DestinationStream;

    /**
     * @return null|array<string, mixed> null when this instance holds live state (a client, memory) and cannot be rebuilt
     */
    public function normalize(): ?array;

    public function getSystemTmpDir(): Path;

    /**
     * @return \Generator<FileStatus>
     */
    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator;

    public function mount(): Mount;

    public function mv(Path $from, Path $to): bool;

    public function readFrom(Path $path): SourceStream;

    /**
     * @throws RuntimeException when $recursive is false and a matched directory is not empty
     */
    public function rm(Path $path, bool $recursive = true): bool;

    public function status(Path $path): ?FileStatus;

    /**
     * True when this filesystem serves the given path's protocol.
     */
    public function supports(Path $path): bool;

    /**
     * Open destination stream for writing, if file already exists it will be overwritten.
     */
    public function writeTo(Path $path): DestinationStream;
}
