<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Mother;

use Flow\ETL\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class WideRowsMother
{
    public static function of(int $columns, int $rows): Rows
    {
        $definitions = [];

        for ($column = 1; $column <= $columns; $column++) {
            $definitions[] = int_schema('c' . $column);
        }

        $data = [];

        for ($id = 1; $id <= $rows; $id++) {
            $row = [];

            for ($column = 1; $column <= $columns; $column++) {
                $row['c' . $column] = $id;
            }

            $data[] = $row;
        }

        return array_to_rows($data, schema(...$definitions));
    }
}
