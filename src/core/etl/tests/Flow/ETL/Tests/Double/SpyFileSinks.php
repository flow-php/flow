<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Backend;
use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Loader\File\FileSinks;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use RuntimeException;

/**
 * Records every open() and every close() by URI; each sink appends "rows:N;" per write and
 * "closed;" at close() to its stream, and throws on write when $failing.
 */
final class SpyFileSinks implements FileSinks
{
    /**
     * @var list<string>
     */
    public array $opened = [];

    /**
     * @var list<string>
     */
    public array $closed = [];

    /**
     * @param bool $failing every write throws
     * @param bool $failingClose every close() throws "close of <uri> failed" once it recorded the close
     */
    public function __construct(
        public readonly bool $failing = false,
        public readonly bool $failingClose = false,
    ) {}

    public function open(DestinationStream $stream, Backend $backend): FileSink
    {
        $this->opened[] = $stream->path()->uri();
        return new readonly class($stream, $this) implements FileSink {
            public function __construct(
                private DestinationStream $stream,
                private SpyFileSinks $spy,
            ) {}

            public function close(): void
            {
                $this->stream->append('closed;');
                $this->spy->closed[] = $this->stream->path()->uri();

                if ($this->spy->failingClose) {
                    throw new RuntimeException('close of ' . $this->stream->path()->uri() . ' failed');
                }
            }

            public function write(Rows $rows): void
            {
                if ($this->spy->failing) {
                    throw new RuntimeException('sink refused the batch');
                }

                $this->stream->append('rows:' . $rows->count() . ';');
            }
        };
    }
}
