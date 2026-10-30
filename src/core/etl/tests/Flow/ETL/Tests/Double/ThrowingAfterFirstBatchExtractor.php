<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;
use RuntimeException;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ThrowingAfterFirstBatchExtractor implements Extractor
{
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        yield array_to_rows([['id' => 1]], $this->schema());

        throw new RuntimeException('source failed after its first batch');
    }

    public function schema(): Schema
    {
        return schema(int_schema('id'));
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
