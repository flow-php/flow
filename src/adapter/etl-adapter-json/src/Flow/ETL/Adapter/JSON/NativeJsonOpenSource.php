<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\SourceStream;
use Generator;

use function strlen;

final readonly class NativeJsonOpenSource implements JsonOpenSource
{
    /**
     * NativeCSVOpenSource::CHUNK's reasoning: each chunk is held as a PHP string, so it sets the read's peak memory.
     */
    public const int CHUNK = 1 << 15;

    /**
     * @param string $head the chunk the opener already read at $offset; the bytes before it are JSON whitespace after
     *                     an optional BOM, which the reader would skip
     * @param int<0, max> $offset
     */
    public function __construct(
        private SourceStream $stream,
        private NativeJsonReader $reader,
        private string $head = '',
        private int $offset = 0,
    ) {}

    public function batches(Schema $schema, int $batchSize, Backend $backend): Generator
    {
        // every batch goes through the configured backend, which keeps its own columns and copies the native ones
        $adopted = static function (Rows $batch) use ($schema, $backend): Rows {
            $columns = [];
            $native = $batch->columns();

            foreach ($schema->definitions() as $name => $definition) {
                $columns[$name] = $backend->adopt($definition, $native[$name]);
            }

            return Rows::fromColumns($schema, $columns, $batch->count());
        };

        $offset = $this->offset;
        $chunk = $this->head === '' ? $this->stream->read(self::CHUNK, $offset) : $this->head;

        while ($chunk !== '') {
            $this->reader->feed($chunk);

            while (($batch = $this->reader->nextColumns($schema, $batchSize)) !== null) {
                yield $adopted($batch);
            }

            $offset += strlen($chunk);
            $chunk = $this->stream->read(self::CHUNK, $offset);
        }

        $this->reader->finish();

        while (($batch = $this->reader->nextColumns($schema, $batchSize)) !== null) {
            yield $adopted($batch);
        }
    }

    public function close(): void
    {
        $this->stream->close();
    }
}
