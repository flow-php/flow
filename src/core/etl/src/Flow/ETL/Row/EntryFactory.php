<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException, SchemaDefinitionNotFoundException};
use Flow\ETL\Row\Schema\Definition;

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
}
