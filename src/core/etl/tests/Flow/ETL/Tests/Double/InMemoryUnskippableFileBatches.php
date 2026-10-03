<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\File\FileBatches;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Schema;
use Generator;

use function array_chunk;
use function array_map;
use function Flow\ETL\DSL\array_to_rows;

/**
 * Each file is a list of `id` values, every one of them yielded whatever the window: a format that cannot skip rows.
 * Records every window it was given, by URI.
 */
final class InMemoryUnskippableFileBatches implements FileBatches
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
        $read = $ids;

        foreach ($read === [] ? [] : array_chunk($read, $batchSize) as $chunk) {
            yield array_to_rows(array_map(static fn(int $id): array => ['id' => $id], $chunk), $body, $backend);
        }

        return 0;
    }
}
