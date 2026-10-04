<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final class DeclaringExtractor implements Extractor
{
    public function __construct(
        public Statistics $statistics,
    ) {}

    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        yield Rows::empty($this->schema(), new AdaptiveBackend());
    }

    public function schema(): Schema
    {
        return new Schema();
    }

    public function statistics(): Statistics
    {
        return $this->statistics;
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
