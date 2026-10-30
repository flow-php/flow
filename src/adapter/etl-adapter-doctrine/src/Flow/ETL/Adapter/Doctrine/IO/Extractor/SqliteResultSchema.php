<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Extractor;

use Flow\ETL\Adapter\Doctrine\Foundation\Exception\ProbeRefusal;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Values\Schema;
use PDO;
use SQLite3;

final readonly class SqliteResultSchema
{
    /**
     * @param class-string $extractor
     *
     * @throws ProbeRefusal
     * @throws SchemaNotDerivableException
     */
    public function of(SQLite3|PDO $connection, string $sql, string $extractor): Schema
    {
        $probe = (new DescribeQuery())->of($sql);
        $names = $connection instanceof SQLite3
            ? (new Sqlite3ColumnNames())->of($connection, $probe)
            : (new PdoSqliteColumnNames())->of($connection, $probe);

        $columns = [];

        foreach ($names as $name) {
            $columns[] = new ResultColumn($name);
        }

        return (new TypedColumns())->schema($columns, new SqliteTypes(), $extractor);
    }
}
