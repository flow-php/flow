<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\native_local_filesystem;

/**
 * A FilesystemBuckets over a fresh scratch directory, removed with it.
 */
final readonly class ScratchBuckets
{
    public function __construct(
        public Path $root,
        public FilesystemRowsStorage $storage,
    ) {}

    /**
     * @param int<1, max> $batchSize
     */
    public static function create(string $name, int $batchSize = 2048): self
    {
        $root = ReaderRoundTrip::scratch($name);

        return new self(
            $root,
            new FilesystemRowsStorage(native_local_filesystem(), $root, new PhpBackend(), $batchSize),
        );
    }

    public function remove(): void
    {
        native_local_filesystem()->rm($this->root);
    }
}
