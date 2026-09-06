<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Row\References;
use Flow\ETL\Schema;

use function array_key_exists;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_optional;

final readonly class PivotSchema
{
    /**
     * @param list<int|string> $pivotColumns
     */
    public function of(Schema $input, References $refs, array $pivotColumns, AggregatingFunction $aggregation): Schema
    {
        $definitions = [];
        $refNames = [];

        foreach ($refs as $ref) {
            $definitions[] = $input->get($ref);
            $refNames[$ref->name()] = true;
        }

        foreach ($pivotColumns as $column) {
            $name = (string) $column;

            if (array_key_exists($name, $refNames)) {
                throw new InvalidArgumentException(
                    'Pivot value "' . $name . '" collides with the group-by column of the same name',
                );
            }

            // the yield loop writes null for a combination it never saw
            $definitions[] = definition_from_type($name, type_optional($aggregation->returns()));
        }

        return new Schema(...$definitions);
    }
}
