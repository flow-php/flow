<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\Bulk\QueryFactory\DbalQueryFactory;

final class BulkInsert
{
    private Connection $connection;

    private QueryFactory $queryFactory;

    public function __construct(Connection $connection, QueryFactory $queryFactory)
    {
        $this->connection = $connection;
        $this->queryFactory = $queryFactory;
    }

    public static function create(Connection $connection) : self
    {
        return new self($connection, new DbalQueryFactory());
    }

    public function insert(string $table, BulkData $bulkData) : void
    {
        $this->connection->executeQuery(
            $this->queryFactory->insert($this->connection->getDatabasePlatform(), $table, $bulkData),
            $bulkData->toSqlParameters(),
            \array_map(
                fn ($value) : string => \gettype($value),
                \array_filter($bulkData->toSqlParameters(), fn ($value) : bool => \is_bool($value))
            )
        );
    }

    public function insertOrSkipOnConflict(string $table, BulkData $bulkData) : void
    {
        $this->connection->executeQuery(
            $this->queryFactory->insertOrSkipOnConflict($this->connection->getDatabasePlatform(), $table, $bulkData),
            $bulkData->toSqlParameters(),
            \array_map(
                fn ($value) : string => \gettype($value),
                \array_filter($bulkData->toSqlParameters(), fn ($value) : bool => \is_bool($value))
            )
        );
    }

    public function insertOrUpdateOnConstraintConflict(string $table, string $constraint, BulkData $bulkData) : void
    {
        $this->connection->executeQuery(
            $this->queryFactory->insertOrUpdateOnConstraintConflict($this->connection->getDatabasePlatform(), $table, $constraint, $bulkData),
            $bulkData->toSqlParameters(),
            \array_map(
                fn ($value) : string => \gettype($value),
                \array_filter($bulkData->toSqlParameters(), fn ($value) : bool => \is_bool($value))
            )
        );
    }
}
