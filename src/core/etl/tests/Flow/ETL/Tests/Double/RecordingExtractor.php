<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_values;

/**
 * A source that lists no files and records the limit of every read.
 */
final class RecordingExtractor implements Extractor
{
    /**
     * @var list<null|int>
     */
    public array $limits = [];

    /**
     * @var list<Rows>
     */
    private readonly array $batches;

    public function __construct(
        private readonly Schema $schema,
        Rows ...$batches,
    ) {
        $this->batches = array_values($batches);
    }

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $this->limits[] = $criteria->limit;

        foreach ($this->batches as $rows) {
            yield $rows;
        }
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function isRepeatable(): bool
    {
        return false;
    }
}
