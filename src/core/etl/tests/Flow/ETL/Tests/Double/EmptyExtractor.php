<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class EmptyExtractor implements Extractor
{
    public function extract(FlowContext $context): Generator
    {
        yield rows(schema());
    }

    public function schema(): Schema
    {
        return new Schema();
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
