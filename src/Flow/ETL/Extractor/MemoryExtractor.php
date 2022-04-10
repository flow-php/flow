<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
 * @psalm-immutable
 */
final class MemoryExtractor implements Extractor
{
    private const CHUNK_SIZE = 100;

    /**
     * @param Memory $memory
     * @param int<1, max> $chunkSize
     * @param string $rowEntryName
     */
    public function __construct(
        private readonly Memory $memory,
        private readonly int $chunkSize = self::CHUNK_SIZE,
        private readonly string $rowEntryName = 'row'
    ) {
    }

    public function extract() : \Generator
    {
        foreach (\array_chunk($this->memory->dump(), $this->chunkSize) as $chunk) {
            $rows = [];

            /**
             * @var array<mixed> $chunkEntry
             */
            foreach ($chunk as $chunkEntry) {
                $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $chunkEntry));
            }

            yield new Rows(...$rows);
        }
    }
}
