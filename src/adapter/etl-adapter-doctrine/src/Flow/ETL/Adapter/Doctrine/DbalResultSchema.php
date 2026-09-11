<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;
use mysqli;
use PDO;
use PgSql\Connection as PgSqlConnection;
use SQLite3;

use function get_debug_type;
use function sprintf;

final readonly class DbalResultSchema
{
    /**
     * The one schema a read has: extract() asks for it before the first row and never falls back to
     * per-batch typing, so a query that cannot be described does not run.
     *
     * @param class-string $extractor
     *
     * @throws SchemaNotDerivableException
     */
    public function of(Connection $connection, string $sql, string $extractor): Schema
    {
        $native = $connection->getNativeConnection();

        return match (true) {
            $native instanceof PgSqlConnection => (new TypedColumns())->schema(
                (new PgSqlResultColumns())->of($native, (new DescribeQuery())->of($sql), $extractor),
                new PgSqlTypesMap(),
                $extractor,
            ),
            $native instanceof mysqli => (new TypedColumns())->schema(
                (new MysqliResultColumns())->of($native, $sql, $extractor),
                new MysqliTypesMap(),
                $extractor,
            ),
            $native instanceof SQLite3,
            $native instanceof PDO && $native->getAttribute(PDO::ATTR_DRIVER_NAME) === 'sqlite',
                => (new SqliteResultSchema())->of($native, $sql, $extractor),
            default => throw SchemaNotDerivableException::extractor($extractor, sprintf(
                'the %s driver reports no column types for a query result, so this query cannot be typed',
                get_debug_type($native),
            )),
        };
    }
}
