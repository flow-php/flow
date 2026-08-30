<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;

final class PhpRowHydrator implements Hydrator
{
    public function cast(array $batch, ?Schema $schema = null): Rows
    {
        if ($schema === null) {
            return (new InferredBatch())->of($batch);
        }

        return (new HydratedBatch())->of(
            $batch,
            $schema,
            static fn(mixed $value, Definition $definition): mixed => $value === null
                ? null
                : $definition->type()->cast($value),
            fillMissing: true,
        );
    }

    public function dehydrate(Rows $rows): array
    {
        $batch = [];

        foreach ($rows as $row) {
            $values = [];
            $types = [];
            $metadata = [];

            foreach ($rows->schema()->definitions() as $definition) {
                $name = $definition->entry()->name();

                // Floe keeps absent and null distinct, so a column the row does not carry is omitted
                if (!$row->has($name)) {
                    continue;
                }

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

    public function hydrate(array $batch, ?Schema $schema = null): Rows
    {
        if ($schema === null) {
            throw new InvalidArgumentException(
                'PhpRowHydrator::hydrate() requires a schema, use cast() to infer from values',
            );
        }

        return (new HydratedBatch())->of(
            $batch,
            $schema,
            static fn(mixed $value, Definition $definition): mixed => $value,
            fillMissing: false,
        );
    }
}
