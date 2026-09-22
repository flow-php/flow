<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Row\RawRowValues;
use Generator;
use IteratorAggregate;

/**
 * @implements IteratorAggregate<int, RawRowValues>
 */
final class JsonFileSample implements IteratorAggregate
{
    private ?JsonSampledRows $sampled = null;

    public function __construct(
        private readonly JsonFileReader $reader,
        private readonly SourceFile $source,
    ) {}

    /**
     * Records what it read when the consumer stops or the file ends, whichever comes first.
     *
     * @return Generator<int, RawRowValues>
     */
    public function getIterator(): Generator
    {
        $read = new JsonReadBytes();
        $rows = 0;
        $wholeFile = false;

        try {
            foreach ($this->reader->sample($this->source, $read) as $row) {
                $rows++;

                yield $row;
            }

            $wholeFile = true;
        } finally {
            $this->sampled = new JsonSampledRows($rows, $read->total(), $wholeFile);
        }
    }

    /**
     * Null until the sample was iterated.
     */
    public function sampled(): ?JsonSampledRows
    {
        return $this->sampled;
    }
}
