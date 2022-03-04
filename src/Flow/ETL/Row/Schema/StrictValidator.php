<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\SchemaValidator;
use Flow\Serializer\Serializable;

/**
 * Matches all entries in the schema, if row comes with any extra entry it will fail validation.
 */
final class StrictValidator implements SchemaValidator, Serializable
{
    /**
     * @phpstan-ignore-next-line
     *
     * @return array
     * @psalm-suppress LessSpecificImplementedReturnType
     */
    public function __serialize() : array
    {
        return [];
    }

    /**
     * @phpstan-ignore-next-line
     *
     * @param array $data
     */
    public function __unserialize(array $data) : void
    {
    }

    public function isValid(Rows $rows, Schema $schema) : bool
    {
        foreach ($rows as $row) {
            if ($schema->count() !== $row->entries()->count()) {
                return false;
            }

            foreach ($row->entries()->all() as $entry) {
                $definition = $schema->findDefinition($entry->name());

                if ($definition === null) {
                    return false;
                }

                if (!$definition->matches($entry)) {
                    return false;
                }
            }
        }

        return true;
    }
}
