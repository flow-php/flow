<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\Types\DSL\type_string;
use Flow\ETL\Row\Entry\{UuidEntry, XMLEntry};
use Flow\ETL\{Rows, Schema};

final readonly class RowsNormalizer
{
    public function __construct()
    {
    }

    /**
     * @param Schema $schema
     *
     * @return array<mixed, array<string, mixed>>
     */
    public function normalize(Rows $rows, Schema $schema) : array
    {
        $normalizedRows = [];

        foreach ($rows as $row) {
            $columns = [];

            foreach ($row->entries() as $entry) {
                if ($schema->get($entry->ref())->isNullable() && $entry->value() === null) {
                    $columns[$entry->name()] = null;

                    continue;
                }

                $columns[$entry->name()] = match ($entry::class) {
                    UuidEntry::class => type_string()->cast($entry->value()),
                    XMLEntry::class => type_string()->cast($entry->value()),
                    default => $schema->get($entry->ref())->type()->cast($entry->value()),
                };
            }

            $normalizedRows[] = $columns;
        }

        return $normalizedRows;
    }
}
