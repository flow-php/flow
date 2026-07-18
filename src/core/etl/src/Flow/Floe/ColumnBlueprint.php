<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\EntryInstantiator;
use Flow\ETL\Schema\Definition;

final readonly class ColumnBlueprint
{
    /**
     * @param Definition<mixed> $definition
     * @param EntryInstantiator<Entry<mixed>> $instantiator
     */
    public function __construct(
        public string $name,
        public Definition $definition,
        public Decoding\ValueDecoder $decoder,
        public EntryInstantiator $instantiator,
    ) {}
}
