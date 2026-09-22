<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Explain;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Cardinality;

use function max;
use function min;

final readonly class ExplainedRows
{
    /**
     * The planner's row estimate for $sql, with the read's own offset and maximum applied. SQLite plans carry no
     * estimate; MariaDB, SQL Server, Oracle and DB2 have no tested reader yet - all of them declare only the maximum.
     *
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|string|Type> $types
     */
    public function of(
        Connection $connection,
        string $sql,
        array $parameters = [],
        array $types = [],
        ?int $maximum = null,
        int $offset = 0,
    ): Cardinality {
        $platform = $connection->getDatabasePlatform();
        $planned = match (true) {
            $platform instanceof PostgreSQLPlatform => (new PostgreSqlExplainedRows())->of(
                $connection,
                $sql,
                $parameters,
                $types,
            ),
            $platform instanceof MySQL80Platform => (new MySqlExplainedRows())->of(
                $connection,
                $sql,
                $parameters,
                $types,
            ),
            default => null,
        };

        if ($planned === null) {
            return new Cardinality(atMost: $maximum);
        }

        $estimate = max(0, $planned - $offset);

        return new Cardinality(
            atMost: $maximum,
            estimate: $maximum === null ? $estimate : min($estimate, $maximum),
            relativeError: Cardinality::DEFAULT_RELATIVE_ERROR,
        );
    }
}
