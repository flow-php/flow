<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\Types\DSL\type_string;
use Flow\ETL\Row\Entry\{JsonEntry, UuidEntry, XMLEntry};
use Flow\ETL\{Rows, Schema};
use Flow\Types\Value\Json;

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
                $definition = $schema->get($entry->ref());

                if ($definition->isNullable() && $entry->value() === null) {
                    $columns[$entry->name()] = null;

                    continue;
                }

                $value = match ($entry::class) {
                    JsonEntry::class => $entry->toString(),
                    UuidEntry::class => type_string()->cast($entry->value()),
                    XMLEntry::class => type_string()->cast($entry->value()),
                    default => $schema->get($entry->ref())->type()->cast($entry->value()),
                };

                if ($value instanceof Json) {
                    $value = $value->toString();
                }

                $columns[$entry->name()] = $value;
            }

            $normalizedRows[] = $columns;
        }

        return $normalizedRows;
    }
}
