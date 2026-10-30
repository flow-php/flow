<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Inference;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonFileReader;
use Flow\ETL\Adapter\JSON\IO\Extractor\JsonReadBytes;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Generator;
use IteratorAggregate;

/**
 * @implements IteratorAggregate<int, array<array-key, mixed>>
 */
final class JsonFileSample implements IteratorAggregate
{
    private int $bytes = 0;

    private bool $complete = false;

    /**
     * @param null|FileSplit $window the byte window of $source to sample, Lines only; null samples it whole
     */
    public function __construct(
        private readonly JsonFileReader $reader,
        private readonly SourceFile $source,
        private readonly ?FileSplit $window = null,
    ) {}

    /**
     * Bytes read so far, line endings included; 0 until the sample was iterated.
     */
    public function bytes(): int
    {
        return $this->bytes;
    }

    /**
     * True once an iteration reached the end of the file; false until then, and when the consumer stopped first.
     */
    public function complete(): bool
    {
        return $this->complete;
    }

    /**
     * Records what it read when the consumer stops or the file ends, whichever comes first.
     *
     * @return Generator<int, array<array-key, mixed>>
     */
    public function getIterator(): Generator
    {
        $read = new JsonReadBytes();

        try {
            foreach ($this->reader->sample($this->source, $read, $this->window) as $row) {
                yield $row;
            }

            $this->complete = true;
        } finally {
            $this->bytes = $read->total();
        }
    }
}
