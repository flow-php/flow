<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\QueryBuilder\Sql;

use function rtrim;

final readonly class DescribeQuery
{
    /**
     * The rtrim strips a trailing semicolon and the newline closes an unterminated line comment —
     * both are shapes Client::cursor() accepts and a naive wrapper turns into a syntax error.
     */
    public function of(Sql|string $sql): string
    {
        return (
            "SELECT * FROM (\n"
            . rtrim($sql instanceof Sql ? $sql->toSql() : $sql, " \t\r\n;")
            . "\n) flow_describe LIMIT 0"
        );
    }
}
