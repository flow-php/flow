<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\SchemaValidator;

/**
 * @implements SchemaValidator<array<mixed>>
 * Matches only entries defined in the schema, ignoring every other entries in the row.
 */
final class SelectiveValidator implements SchemaValidator
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function isValid(Rows $rows, Schema $schema) : bool
    {
        foreach ($schema->entries() as $ref) {
            /** @var Definition $definition */
            $definition = $schema->getDefinition($ref);

            foreach ($rows as $row) {
                try {
                    $entry = $row->entries()->get($ref);

                    if (!$definition->matches($entry)) {
                        return false;
                    }
                } catch (InvalidArgumentException) {
                    return false;
                }
            }
        }

        return true;
    }
}
