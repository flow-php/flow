<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\QueryFactory\DbalQueryFactory;

use function count;
use function intdiv;
use function max;

final readonly class Bulk
{
    private BulkStatement $statement;

    public function __construct(
        QueryFactory $queryFactory,
        private TableDefinitions $tableDefinitions,
    ) {
        $this->statement = new BulkStatement($queryFactory);
    }

    public static function create(): self
    {
        return new self(new DbalQueryFactory(), new TableDefinitions());
    }

    /**
     * Delete data from the database in bulk based on the provided bulk data.
     *
     * @throws Exception|RuntimeException
     */
    public function delete(Connection $connection, string $table, BulkData $bulkData): void
    {
        $tableDefinition = $this->tableDefinitions->get($table, $connection);
        $chunks = $bulkData->chunk(max(1, intdiv(
            (new DbalPlatform($connection->getDatabasePlatform()))->dialect()->maxBindParameters(),
            $bulkData->columns()->count(),
        )));

        if (count($chunks) === 1) {
            $this->statement->delete($connection, $tableDefinition, $chunks[0]);

            return;
        }

        $connection->transactional(function (Connection $connection) use ($tableDefinition, $chunks): void {
            foreach ($chunks as $chunk) {
                $this->statement->delete($connection, $tableDefinition, $chunk);
            }
        });
    }

    /**
     * Insert data into the database in bulk.
     * Insert should be used whenever you want to insert a large number of rows into a table or upsert them.
     *
     * Each database platform has its own way of handling bulk inserts, please make sure to use the correct implementation of InsertOptions for your platform.
     *
     * @throws Exception|RuntimeException
     */
    public function insert(
        Connection $connection,
        string $table,
        BulkData $bulkData,
        ?InsertOptions $options = null,
    ): void {
        $tableDefinition = $this->tableDefinitions->get($table, $connection);
        $chunks = $bulkData->chunk(max(1, intdiv(
            (new DbalPlatform($connection->getDatabasePlatform()))->dialect()->maxBindParameters(),
            $bulkData->columns()->count(),
        )));

        if (count($chunks) === 1) {
            $this->statement->insert($connection, $tableDefinition, $chunks[0], $options);

            return;
        }

        $connection->transactional(function (Connection $connection) use ($tableDefinition, $chunks, $options): void {
            foreach ($chunks as $chunk) {
                $this->statement->insert($connection, $tableDefinition, $chunk, $options);
            }
        });
    }

    /**
     * @param Connection $connection
     * @param string $table
     * @param BulkData $bulkData
     *
     * @throws Exception|RuntimeException
     */
    public function update(
        Connection $connection,
        string $table,
        BulkData $bulkData,
        ?UpdateOptions $options = null,
    ): void {
        $tableDefinition = $this->tableDefinitions->get($table, $connection);
        $chunks = $bulkData->chunk(max(1, intdiv(
            (new DbalPlatform($connection->getDatabasePlatform()))->dialect()->maxBindParameters(),
            $bulkData->columns()->count(),
        )));

        if (count($chunks) === 1) {
            $this->statement->update($connection, $tableDefinition, $chunks[0], $options);

            return;
        }

        $connection->transactional(function (Connection $connection) use ($tableDefinition, $chunks, $options): void {
            foreach ($chunks as $chunk) {
                $this->statement->update($connection, $tableDefinition, $chunk, $options);
            }
        });
    }
}
