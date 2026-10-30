<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Extractor;

use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definitions;

use function array_values;
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
            $definitions[$column->name] = Definitions::of(
                $column->name,
                $types->toFlowType($column->native) ?? throw SchemaNotDerivableException::extractor($extractor, sprintf(
                    'column "%s" has driver type "%s", which Flow has no type for',
                    $column->name,
                    $column->native,
                )),
                nullable: true,
            );
        }

        return new Schema(...array_values($definitions));
    }
}
