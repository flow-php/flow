<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;

use function array_values;
use function Flow\ETL\DSL\definition_from_type;
use function sprintf;

final readonly class TypedColumns
{
    /**
     * Every column is nullable: result metadata cannot prove NOT NULL, and an outer join or an
     * expression can always produce null. Duplicate output names collapse last-wins, matching what
     * fetchAssociative() does to the row itself.
     *
     * @param list<ResultColumn> $columns
     * @param class-string $extractor
     *
     * @throws SchemaNotDerivableException when a column has a driver type this adapter cannot map
     */
    public function schema(array $columns, NativeTypes $types, string $extractor): Schema
    {
        $definitions = [];

        foreach ($columns as $column) {
            $type =
                $types->toFlowType($column->native) ?? throw SchemaNotDerivableException::extractor($extractor, sprintf(
                    'column "%s" has driver type "%s", which Flow has no type for',
                    $column->name,
                    $column->native,
                ));

            $definitions[$column->name] = definition_from_type($column->name, $type, nullable: true);
        }

        return new Schema(...array_values($definitions));
    }
}
