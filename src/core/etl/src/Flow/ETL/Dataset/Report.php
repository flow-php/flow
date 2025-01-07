<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset;

use Flow\ETL\Row\Schema;

final readonly class Report
{
    public function __construct(
        private Schema $schema,
        private Statistics $statistics,
    ) {

    }

    public function schema() : Schema
    {
        return $this->schema;
    }

    public function statistics() : Statistics
    {
        return $this->statistics;
    }
}
