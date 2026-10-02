<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\RustIterator;
use Flow\ETL\Schema;
use Flow\Filesystem\SourceStream;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class RustJsonOpenSource implements JsonOpenSource
{
    /**
     * @param string $head the chunk already read at $offset ('' for none); the bytes before it are JSON whitespace after
     *                     an optional BOM, which the reader would skip
     * @param int<0, max> $offset
     */
    public function __construct(SourceStream $stream, bool $lines, string $uri, string $head, int $offset)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * @return RustIterator<Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend): RustIterator
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function close(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}
