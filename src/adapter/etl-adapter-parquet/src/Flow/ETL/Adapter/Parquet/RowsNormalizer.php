<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;

final class RowsNormalizer
{
    public function __construct(private readonly Caster $caster)
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
                    default => $this->caster->to($schema->getDefinition($entry->ref())->type())->value($entry->value()),
                };
            }

            $normalizedRows[] = $columns;
        }

        return $normalizedRows;
    }
}
