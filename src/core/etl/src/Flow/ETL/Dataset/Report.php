<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset;

use Flow\ETL\Row\Schema;

final class Report
{
    public function __construct(
        private readonly Schema $schema,
        private readonly Statistics $statistics,
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
