<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Exception;
use SQLite3;

use function Flow\Types\DSL\type_string;
use function sprintf;

final readonly class Sqlite3ColumnNames
{
    /**
     * @throws ProbeRefusal
     *
     * @return list<string>
     */
    public function of(SQLite3 $connection, string $probe): array
    {
        try {
            $statement = $connection->prepare($probe) ?: throw new Exception($connection->lastErrorMsg());
            $result = $statement->execute() ?: throw new Exception($connection->lastErrorMsg());

            $names = [];

            for ($i = 0; $i < $result->numColumns(); $i++) {
                $names[] = type_string()->assert($result->columnName($i));
            }

            $result->finalize();
            $statement->close();

            return $names;
        } catch (Exception $e) {
            throw new ProbeRefusal(
                sprintf('SQLite refused the zero-row probe of this query (%s)', $e->getMessage()),
                $connection->lastErrorCode(),
                null,
                $e,
            );
        }
    }
}
