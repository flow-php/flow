<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;

/**
 * Models Avro's reason-less refusal - the only reason-less producer in src/.
 * An empty read of one of these must stay empty rather than start throwing.
 */
final class UndescribableRowLessExtractor implements Extractor
{
    public function extract(FlowContext $context): Generator
    {
        yield from [];
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
