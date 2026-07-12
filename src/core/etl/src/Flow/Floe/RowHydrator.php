<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\Serializer\Exception\SerializationException;

use function ord;
use function sprintf;

final class RowHydrator
{
    /**
     * @param array<int, ColumnBlueprint> $plan
     */
    public function hydrate(array $plan, string $data, int &$position): Row
    {
        $entries = [];

        foreach ($plan as $column) {
            $flag = ord($data[$position++]);

            if ($flag === Format::VALUE_ABSENT) {
                // The row had no such column; reconstruct it as written (omitted).
                continue;
            }

            if ($flag === Format::VALUE_PRESENT) {
                // @mago-ignore analysis:mixed-assignment
                $value = $column->decoder->decode($data, $position);
                $definition = clone $column->definition;
            } elseif ($flag === Format::VALUE_NULL) {
                $value = null;
                $definition = clone $column->nullableDefinition;
            } elseif ($flag === Format::VALUE_NULL_FROM_NULL) {
                $value = null;
                $definition = clone $column->fromNullDefinition;
            } else {
                throw new SerializationException(sprintf('Floe found unknown value flag 0x%02X', $flag));
            }

            $entries[$column->name] = $column->instantiator->instantiate($column->name, $value, $definition);
        }

        return new Row(Entries::recreate($entries));
    }
}
