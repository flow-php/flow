<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException, SchemaDefinitionNotFoundException};
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\{Definition, Metadata};

interface EntryFactory
{
    /**
     * @throws InvalidArgumentException
     * @throws RuntimeException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Entry<mixed, mixed>
     */
    public function create(string $entryName, mixed $value, Schema|Definition|null $schema = null) : Entry;

    /**
     * @param Type<mixed> $type
     *
     * @return Entry<mixed, mixed>
     */
    public function createAs(string $entryName, mixed $value, Type $type, ?Metadata $metadata = null) : Entry;
}
