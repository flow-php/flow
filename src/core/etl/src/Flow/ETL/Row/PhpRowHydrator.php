<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Rows;
use Flow\ETL\Schema;

final class PhpRowHydrator implements Hydrator
{
    public function dehydrate(Rows $rows): array
    {
        $batch = [];

        foreach ($rows as $row) {
            $values = [];
            $types = [];
            $metadata = [];

            foreach ($rows->schema()->definitions() as $definition) {
                $name = $definition->entry()->name();

                $values[$name] = $row->get($name);
                $types[$name] = $definition->type();

                if (!$definition->metadata()->isEmpty()) {
                    $metadata[$name] = $definition->metadata();
                }
            }

            $batch[] = new TypedRowValues($values, $types, $metadata);
        }

        return $batch;
    }

    public function hydrate(array $batch, Schema $schema): Rows
    {
        return (new HydratedBatch())->of($batch, $schema);
    }
}
