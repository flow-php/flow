<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;
use RuntimeException;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ThrowingAfterFirstBatchExtractor implements Extractor
{
    public function extract(FlowContext $context, ?int $limit = null): Generator
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
}
