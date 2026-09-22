<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset;

use Flow\ETL\Schema;

final readonly class Report
{
    /**
     * @param null|list<SourceStatistics> $sources null when the run was not analyzed with source statistics
     */
    public function __construct(
        private ?Schema $schema,
        private Statistics $statistics,
        private ?array $sources,
    ) {}

    /**
     * @return null|list<SourceStatistics>
     */
    public function sources(): ?array
    {
        return $this->sources;
    }

    public function schema(): ?Schema
    {
        return $this->schema;
    }

    public function statistics(): Statistics
    {
        return $this->statistics;
    }
}
