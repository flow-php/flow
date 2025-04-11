<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row\Entry\{UuidEntry, XMLEntry};
use Flow\ETL\{Rows, Schema};

final readonly class RowsNormalizer
{
    public function __construct(private Caster $caster)
    {
    }

    /**
     * @return array<mixed, array<string, mixed>>
     */
    public function normalize(Rows $rows, Schema $schema) : array
    {
        $normalizedRows = [];

        foreach ($rows as $row) {
            $columns = [];

            foreach ($row->entries() as $entry) {
                $columns[$entry->name()] = match ($entry::class) {
                    UuidEntry::class => $this->caster->to(type_string())->value($entry->value()),
                    XMLEntry::class => $this->caster->to(type_string())->value($entry->value()),
                    default => $this->caster->to($schema->get($entry->ref())->type())->value($entry->value()),
                };
            }

            $normalizedRows[] = $columns;
        }

        return $normalizedRows;
    }
}
