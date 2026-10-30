<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;

use function Flow\ETL\DSL\from_rows;

/**
 * Models Avro's reason-less refusal - the only reason-less producer in src/.
 * An empty read of one of these must stay empty rather than start throwing.
 */
final class UndescribableRowLessExtractor implements Extractor
{
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        return from_rows()->extract($backend, new Criteria());
    }

    public function schema(): Schema
    {
        throw SchemaNotDerivableException::extractor(self::class);
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
