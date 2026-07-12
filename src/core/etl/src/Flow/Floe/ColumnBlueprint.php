<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\EntryInstantiator;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;

final readonly class ColumnBlueprint
{
    /**
     * @var Definition<mixed>
     */
    public Definition $nullableDefinition;

    /**
     * @var Definition<mixed>
     */
    public Definition $fromNullDefinition;

    /**
     * @param Definition<mixed> $definition
     * @param EntryInstantiator<Entry<mixed>> $instantiator
     */
    public function __construct(
        public string $name,
        public Definition $definition,
        public Decoding\ValueDecoder $decoder,
        public EntryInstantiator $instantiator,
    ) {
        $this->fromNullDefinition = $definition
            ->makeNullable()
            ->setMetadata($definition->metadata()->merge(Metadata::fromArray([Metadata::FROM_NULL => true])));
        $this->nullableDefinition = $definition->makeNullable();
    }
}
