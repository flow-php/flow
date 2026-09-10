<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;
use RuntimeException;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ThrowingAfterFirstBatchExtractor implements Extractor
{
    public function extract(FlowContext $context): Generator
    {
        yield rows($this->schema(), row(['id' => 1]));

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
}
