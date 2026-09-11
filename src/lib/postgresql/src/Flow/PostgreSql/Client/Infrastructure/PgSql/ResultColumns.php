<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Infrastructure\PgSql;

use PgSql\Result;

use function pg_field_name;
use function pg_field_type;
use function pg_num_fields;

final readonly class ResultColumns
{
    /**
     * Result-set columns in select order. Available with zero rows.
     *
     * @return list<array{name: string, type: string}>
     */
    public function of(Result $result): array
    {
        $columns = [];
        $count = pg_num_fields($result);

        for ($i = 0; $i < $count; $i++) {
            $columns[] = [
                'name' => pg_field_name($result, $i),
                'type' => pg_field_type($result, $i),
            ];
        }

        return $columns;
    }
}
