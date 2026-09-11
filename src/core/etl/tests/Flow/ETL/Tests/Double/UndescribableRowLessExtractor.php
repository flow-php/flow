<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\from_rows;

/**
 * Models Avro's reason-less refusal - the only reason-less producer in src/.
 * An empty read of one of these must stay empty rather than start throwing.
 */
final class UndescribableRowLessExtractor implements Extractor
{
    public function extract(FlowContext $context): Generator
    {
        return from_rows()->extract($context);
    }

    public function schema(): Schema
    {
        throw SchemaNotDerivableException::extractor(self::class);
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
