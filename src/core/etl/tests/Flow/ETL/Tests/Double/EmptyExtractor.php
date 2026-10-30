<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;

use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class EmptyExtractor implements Extractor
{
    public function extract(Backend $backend, Criteria $criteria): Generator
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

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function isRepeatable(): bool
    {
        return false;
    }
}
