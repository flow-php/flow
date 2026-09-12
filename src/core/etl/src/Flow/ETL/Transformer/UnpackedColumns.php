<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Schema;

final readonly class UnpackedColumns
{
    /**
     * One nullable column per declared column of $declared, named "$prefix$column", added to (or
     * replacing in) $base in declaration order.
     */
    public function of(Schema $base, string $prefix, Schema $declared): Schema
    {
        $output = $base;

        foreach ($declared->makeNullable()->definitions() as $definition) {
            $name = $prefix . $definition->entry()->name();

            $output = $output->findDefinition($name) === null
                ? $output->add($definition->rename($name))
                : $output->replace($name, $definition->rename($name));
        }

        return $output;
    }

    /**
     * @param array<string, mixed> $values
     * @param array<array-key, mixed> $payload
     *
     * @return array<string, mixed>
     */
    public function values(array $values, string $prefix, Schema $declared, array $payload): array
    {
        foreach ($declared->definitions() as $name => $definition) {
            // @mago-ignore analysis:mixed-assignment
            $value = $payload[$name] ?? null;
            $values[$prefix . $name] = $value === null ? null : $definition->type()->cast($value);
        }

        return $values;
    }
}
