<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Exception\SchemaNotDerivableException;
use PDO;
use PDOException;

use function Flow\Types\DSL\type_string;
use function sprintf;

final readonly class PdoSqliteColumnNames
{
    /**
     * @param class-string $extractor
     *
     * @throws SchemaNotDerivableException
     *
     * @return list<string>
     */
    public function of(PDO $connection, string $probe, string $extractor): array
    {
        try {
            $statement = $connection->prepare($probe) ?: throw new PDOException('prepare() returned false');
            $statement->execute();

            $names = [];

            for ($i = 0; $i < $statement->columnCount(); $i++) {
                $names[] = type_string()->assert(
                    ($statement->getColumnMeta($i) ?: throw new PDOException('no column metadata'))['name'],
                );
            }

            $statement->closeCursor();

            return $names;
        } catch (PDOException $e) {
            throw SchemaNotDerivableException::extractor($extractor, sprintf(
                'SQLite refused the zero-row probe of this query (%s)',
                $e->getMessage(),
            ));
        }
    }
}
