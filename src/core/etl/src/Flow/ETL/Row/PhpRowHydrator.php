<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;

use function array_map;

final readonly class PhpRowHydrator implements Hydrator
{
    public function __construct(
        private Backend $backend = new DefaultBackend(),
    ) {}

    public function dehydrate(Rows $rows): array
    {
        $values = [];
        $types = [];
        $metadata = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $name = $definition->entry()->name();
            $values[$name] = $rows->column($name)->values();
            $types[$name] = $definition->type();

            if (!$definition->metadata()->isEmpty()) {
                $metadata[$name] = $definition->metadata();
            }
        }

        $batch = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $row = [];

            foreach ($values as $name => $column) {
                $row[$name] = $column[$i];
            }

            $batch[] = new TypedRowValues($row, $types, $metadata);
        }

        return $batch;
    }

    public function hydrate(array $batch, Schema $schema): Rows
    {
        // metadata belongs to the column, so it is folded into the schema once - a per-row divergent Metadata is
        // no longer representable
        foreach ($batch as $rowValues) {
            foreach ($rowValues->metadata as $name => $metadata) {
                // PHP casts a numeric array key to int, column names are always strings
                $name = (string) $name;

                if ($schema->findDefinition($name) !== null) {
                    $schema = $schema->setMetadata($name, $metadata);
                }
            }
        }

        return (new RowsBuilder($schema, $this->backend))
            ->appendRows(array_map(static fn(RawRowValues $row): array => $row->values, $batch))
            ->finish();
    }
}
