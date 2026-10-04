<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class RowsMother
{
    /**
     * 3 batches of 2 rows, ids descending: [5,4][3,2][1,0].
     *
     * @return list<Rows>
     */
    public static function descendingIdBatches(): array
    {
        return [
            array_to_rows([['id' => 5], ['id' => 4]], schema(int_schema('id'))),
            array_to_rows([['id' => 3], ['id' => 2]], schema(int_schema('id'))),
            array_to_rows([['id' => 1], ['id' => 0]], schema(int_schema('id'))),
        ];
    }

    /**
     * 3 batches of 2 rows, groups interleaved: [a1,b10][a2,b20][a3,b30].
     *
     * @return list<Rows>
     */
    public static function interleavedGroupBatches(): array
    {
        return [
            array_to_rows([['g' => 'a', 'v' => 1], ['g' => 'b', 'v' => 10]], schema(str_schema('g'), int_schema('v'))),
            array_to_rows([['g' => 'a', 'v' => 2], ['g' => 'b', 'v' => 20]], schema(str_schema('g'), int_schema('v'))),
            array_to_rows([['g' => 'a', 'v' => 3], ['g' => 'b', 'v' => 30]], schema(str_schema('g'), int_schema('v'))),
        ];
    }

    /**
     * 3 batches of 2 rows, groups already consecutive but straddling batch boundaries:
     * [a1,a2][a3,b10][b20,b30].
     *
     * @return list<Rows>
     */
    public static function sortedGroupBatches(): array
    {
        return [
            array_to_rows([['g' => 'a', 'v' => 1], ['g' => 'a', 'v' => 2]], schema(str_schema('g'), int_schema('v'))),
            array_to_rows([['g' => 'a', 'v' => 3], ['g' => 'b', 'v' => 10]], schema(str_schema('g'), int_schema('v'))),
            array_to_rows([['g' => 'b', 'v' => 20], ['g' => 'b', 'v' => 30]], schema(str_schema('g'), int_schema('v'))),
        ];
    }

    public static function sequentialIds(int $count): Rows
    {
        $sequence = [];

        for ($id = 1; $id <= $count; $id++) {
            $sequence[] = ['id' => $id];
        }

        return array_to_rows($sequence, schema(int_schema('id')));
    }

    /**
     * One row whose "array_entry" structure nests a structure: {id, status, enabled, array: {foo}}.
     */
    public static function arrayEntry(): Rows
    {
        return array_to_rows([[
            'array_entry' => ['id' => 1, 'status' => 'PENDING', 'enabled' => true, 'array' => ['foo' => 'bar']],
        ]], schema(structure_schema('array_entry', type_structure([
            'id' => type_integer(),
            'status' => type_string(),
            'enabled' => type_boolean(),
            'array' => type_structure(['foo' => type_string()]),
        ]))));
    }
}
