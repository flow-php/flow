<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\File\OffsetSkippingFileBatches;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Schema;
use Generator;

use function array_chunk;
use function array_map;
use function array_slice;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function min;

/**
 * Each file is a list of `id` values. Skips the window's offset, honours its limit and records every window it was
 * given, by URI.
 */
final class InMemoryFileBatches implements OffsetSkippingFileBatches
{
    /**
     * @var array<string, list<ReadWindow>>
     */
    public array $windows = [];

    /**
     * @param array<string, list<int>> $ids by URI
     */
    public function __construct(
        private readonly array $ids,
    ) {}

    public function batches(
        SourceFile $source,
        Schema $body,
        int $batchSize,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $this->windows[$source->uri()][] = $window;
        $ids = $this->ids[$source->uri()] ?? [];
        $read = array_slice($ids, min($window->offset, count($ids)), $window->limit);

        foreach ($read === [] ? [] : array_chunk($read, $batchSize) as $chunk) {
            yield array_to_rows(array_map(static fn(int $id): array => ['id' => $id], $chunk), $body, $backend);
        }

        return min($window->offset, count($ids));
    }
}
