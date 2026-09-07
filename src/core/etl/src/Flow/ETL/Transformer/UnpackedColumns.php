<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Schema;

/**
 * One nullable column per declared column of $declared, named "$prefix$column", added to (or
 * replacing in) $base in declaration order.
 */
final readonly class UnpackedColumns
{
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
}
