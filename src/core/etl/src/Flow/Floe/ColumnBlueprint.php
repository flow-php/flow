<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Schema\Definition;

final readonly class ColumnBlueprint
{
    /**
     * @param Definition<mixed> $definition
     */
    public function __construct(
        public string $name,
        public Definition $definition,
        public Decoding\ValueDecoder $decoder,
    ) {}
}
