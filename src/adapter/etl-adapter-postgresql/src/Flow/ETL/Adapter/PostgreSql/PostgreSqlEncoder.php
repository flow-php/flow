<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Rows;

final class PostgreSqlEncoder
{
    /**
     * @return list<array<string, mixed>>
     */
    public function encode(Rows $rows): array
    {
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $columns[$definition->entry()->name()] = $rows->column($definition->entry()->name())->values();
        }

        $encoded = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $row = [];

            foreach ($columns as $name => $column) {
                $row[$name] = $column[$i];
            }

            $encoded[] = $row;
        }

        return $encoded;
    }
}
