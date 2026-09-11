<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;
use PDO;
use SQLite3;

final readonly class SqliteResultSchema
{
    /**
     * @param class-string $extractor
     *
     * @throws SchemaNotDerivableException
     */
    public function of(SQLite3|PDO $connection, string $sql, string $extractor): Schema
    {
        $probe = (new DescribeQuery())->of($sql);
        $names = $connection instanceof SQLite3
            ? (new Sqlite3ColumnNames())->of($connection, $probe, $extractor)
            : (new PdoSqliteColumnNames())->of($connection, $probe, $extractor);

        $columns = [];

        foreach ($names as $name) {
            $columns[] = new ResultColumn($name);
        }

        return (new TypedColumns())->schema($columns, new SqliteTypes(), $extractor);
    }
}
