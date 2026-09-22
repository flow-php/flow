<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
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

    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $this->limits[] = $limit;

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
}
