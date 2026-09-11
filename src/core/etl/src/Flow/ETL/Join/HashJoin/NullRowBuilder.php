<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Row;
use Flow\ETL\Schema;

use function array_fill_keys;

final readonly class NullRowBuilder
{
    public function __construct(
        private Schema $schema,
    ) {}

    public function row(): Row
    {
        return new Row(array_fill_keys($this->schema->references()->names(), null));
    }
}
