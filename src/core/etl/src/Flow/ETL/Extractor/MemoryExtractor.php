<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Memory\Memory;

final class MemoryExtractor implements Extractor
{
    private const CHUNK_SIZE = 100;

    /**
     * @param Memory $memory
     * @param int<1, max> $chunkSize
     */
    public function __construct(
        private readonly Memory $memory,
        private readonly int $chunkSize = self::CHUNK_SIZE,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach (\array_chunk($this->memory->dump(), $this->chunkSize) as $chunk) {
            $rows = [];

            /**
             * @var array<mixed> $chunkEntry
             */
            foreach ($chunk as $chunkEntry) {
                $rows[] = $chunkEntry;
            }

            yield array_to_rows($rows, $context->entryFactory());
        }
    }
}
