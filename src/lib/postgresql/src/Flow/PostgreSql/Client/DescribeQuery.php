<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\QueryBuilder\Sql;

use function mb_strlen;
use function rtrim;

final readonly class DescribeQuery
{
    private const string PREFIX = "SELECT * FROM (\n";

    private const string SUFFIX = "\n) flow_describe LIMIT 0";

    /**
     * The rtrim strips a trailing semicolon and the newline closes an unterminated line comment —
     * both are shapes Client::cursor() accepts and a naive wrapper turns into a syntax error.
     */
    public function of(Sql|string $sql): string
    {
        return self::PREFIX . rtrim($sql instanceof Sql ? $sql->toSql() : $sql, " \t\r\n;") . self::SUFFIX;
    }

    /**
     * PostgreSQL counts the position in characters of the statement it received; the caller sent $sql, not the wrapper.
     * One past the caller's last character stays valid - that is where "at end of input" points.
     */
    public function errorIn(Sql|string $sql, PostgreSqlError $error): PostgreSqlError
    {
        $position = $error->position === null ? null : $error->position - mb_strlen(self::PREFIX);

        return $error->withPosition(
            $position !== null
            && $position >= 1
            && $position <= (mb_strlen($this->of($sql)) - mb_strlen(self::PREFIX) - mb_strlen(self::SUFFIX) + 1)
                ? $position
                : null,
        );
    }
}
