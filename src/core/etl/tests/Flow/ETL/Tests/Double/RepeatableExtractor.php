<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

/**
 * Answers isRepeatable() with whatever the test needs.
 */
final class RepeatableExtractor implements Extractor
{
    public function __construct(
        private readonly bool $repeatable,
    ) {}

    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        yield Rows::empty($this->schema(), new AdaptiveBackend());
    }

    public function isRepeatable(): bool
    {
        return $this->repeatable;
    }

    public function schema(): Schema
    {
        return new Schema();
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
