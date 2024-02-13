<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;

interface EntryFactory
{
    /**
     * @throws InvalidArgumentException
     * @throws RuntimeException
     * @throws SchemaDefinitionNotFoundException
     */
    public function create(string $entryName, mixed $value, ?Schema $schema = null) : Entry;
}
