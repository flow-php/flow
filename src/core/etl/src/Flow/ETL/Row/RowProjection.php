<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Row;

use function array_key_exists;

/**
 * Rewrites a Row so its storage matches a shape change made to the batch Schema. A Row cannot
 * reshape itself - the operator that changed the Schema projects the rows through this.
 */
final readonly class RowProjection
{
    /**
     * @param array<string> $names in the Schema's order - the projection carries that order into the
     *                             row, so a reorder on the Schema is not silently dropped here
     */
    public function keep(Row $row, array $names): Row
    {
        $values = $row->values();
        $kept = [];

        foreach ($names as $name) {
            if (array_key_exists($name, $values)) {
                $kept[$name] = $values[$name];
            }
        }

        return new Row($kept);
    }

    /**
     * @param array<string, string> $renames current_name => new_name
     */
    public function rename(Row $row, array $renames): Row
    {
        if ($renames === []) {
            return $row;
        }

        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($row->values() as $name => $value) {
            $values[$renames[$name] ?? $name] = $value;
        }

        return new Row($values);
    }
}
