<?php

declare(strict_types=1);

namespace Flow\ETL\Rows;

use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Rows;
use Flow\Types\Type\TypedValueFormatter;

use function array_fill;
use function array_map;

final readonly class RowHashes
{
    /**
     * One hash per row over its values in sorted column order - name followed by the value's equality form
     * (ComparableValues) where the column has one, its formatted value otherwise.
     *
     * @return list<string>
     */
    public function of(Rows $rows, Algorithm $algorithm = new NativePHPHash()): array
    {
        if ($rows->isEmpty()) {
            return [];
        }

        $formatter = new TypedValueFormatter();
        $comparable = new ComparableValues();
        // @mago-ignore analysis:possibly-invalid-argument
        $strings = array_fill(0, $rows->count(), '');

        foreach ($rows->schema()->sort()->definitions() as $definition) {
            $name = $definition->entry()->name();
            $type = $definition->type();
            $column = $rows->column($name);

            if ($comparable->equalByPhysical($type)) {
                // @mago-ignore analysis:mixed-assignment
                foreach ($comparable->equality($column) as $i => $value) {
                    $strings[$i] .= $name . NativeHasher::normalize($value);
                }

                continue;
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $i => $value) {
                $strings[$i] .= $name . $formatter->format($type, $value);
            }
        }

        return array_map(static fn(string $string): string => $algorithm->hash($string), $strings);
    }
}
