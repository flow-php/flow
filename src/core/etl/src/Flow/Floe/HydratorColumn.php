<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Schema\Definition;

final readonly class HydratorColumn
{
    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $nullableDefinition
     * @param Definition<mixed> $fromNullDefinition
     */
    public function __construct(
        public string $name,
        public Definition $definition,
        public Definition $nullableDefinition,
        public Definition $fromNullDefinition,
        public Decoding\ValueDecoder $decoder,
        public EntryFactory $entryFactory,
    ) {}
}
