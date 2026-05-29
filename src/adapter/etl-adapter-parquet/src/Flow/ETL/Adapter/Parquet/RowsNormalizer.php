<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\OptionalType;

use function Flow\Types\DSL\type_string;

final readonly class RowsNormalizer
{
    public function __construct() {}

    /**
     * @param Schema $schema
     *
     * @return array<array-key, array<string, mixed>>
     */
    public function normalize(Rows $rows, Schema $schema): array
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

                if ($entry instanceof JsonEntry) {
                    $columns[$entry->name()] = $entry->toString();
                } elseif ($entry instanceof UuidEntry || $entry instanceof XMLEntry) {
                    $columns[$entry->name()] = type_string()->cast($entry->value());
                } else {
                    $type = $definition->type();
                    $isJsonType =
                        $type instanceof JsonType || $type instanceof OptionalType && $type->base() instanceof JsonType;

                    $columns[$entry->name()] = $isJsonType
                        ? type_string()->cast($type->cast($entry->value()))
                        : $type->cast($entry->value());
                }
            }

            $normalizedRows[] = $columns;
        }

        return $normalizedRows;
    }
}
