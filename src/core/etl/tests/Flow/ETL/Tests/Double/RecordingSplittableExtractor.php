<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\Reader;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function count;
use function Flow\Filesystem\DSL\path;

/**
 * A parallel-capable source that is not a file source: it reads its batches in-process and records what it was asked.
 */
final class RecordingSplittableExtractor implements SplittableExtractor
{
    /**
     * @var list<null|int>
     */
    public array $limits = [];

    /**
     * @var list<Signal|null>
     */
    public array $signals = [];

    /**
     * @param list<Rows> $batches
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly array $batches,
    ) {}

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $this->limits[] = $criteria->limit;

        foreach ($this->batches as $batch) {
            $this->signals[] = yield $batch;

            if ($this->signals[count($this->signals) - 1] === Signal::STOP) {
                return;
            }
        }
    }

    public function reader(): Reader
    {
        return new class($this->batches) implements Reader {
            /**
             * @param list<Rows> $batches
             */
            public function __construct(
                private readonly array $batches,
            ) {}

            public function extract(Backend $backend, Criteria $criteria): Generator
            {
                yield from $this->batches;
            }
        };
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function splits(Criteria $criteria, int $targetSplitBytes): array
    {
        return [new FileSplit(path('memory://source'), 0, null)];
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }

    public function isRepeatable(): bool
    {
        return false;
    }
}
