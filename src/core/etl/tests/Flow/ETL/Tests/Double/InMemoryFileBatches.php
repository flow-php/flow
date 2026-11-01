<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\OffsetSkippingFileBatches;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SkippedRows;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function array_chunk;
use function array_key_exists;
use function array_map;
use function array_slice;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function in_array;
use function min;

final class InMemoryFileBatches implements OffsetSkippingFileBatches
{
    /**
     * @var array<string, list<ReadWindow>>
     */
    public array $windows = [];

    public int $closes = 0;

    /**
     * @param array<string, list<int>> $ids by URI
     * @param array<string, Filter> $residuals by URI
     * @param array<string, CorruptedRecordException> $corrupted by URI
     * @param list<string> $answering URIs whose ids all match the conjuncts, so the window is taken beside them
     */
    public function __construct(
        private readonly array $ids,
        private readonly array $residuals = [],
        private readonly array $corrupted = [],
        private readonly array $answering = [],
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $this->windows[$source->uri()][] = $window;
        $ids = $this->ids[$source->uri()] ?? [];
        $takesWindow = $criteria->conjuncts === [] || in_array($source->uri(), $this->answering, true);
        $skipped = $takesWindow ? min($window->offset, count($ids)) : 0;
        $kept = array_slice($ids, $skipped, $takesWindow ? $window->limit : null);

        if (array_key_exists($source->uri(), $this->residuals)) {
            yield $this->residuals[$source->uri()];
        }

        if ($takesWindow && $window->offset > 0) {
            yield new SkippedRows($skipped);
        }

        if (array_key_exists($source->uri(), $this->corrupted)) {
            yield $this->corrupted[$source->uri()];
        }

        foreach ($kept === [] ? [] : array_chunk($kept, $read->batchSize) as $chunk) {
            yield array_to_rows(array_map(static fn(int $id): array => ['id' => $id], $chunk), $body, $backend);
        }
    }

    public function close(): void
    {
        $this->closes++;
    }
}
