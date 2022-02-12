<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\BulkInsert;
use Flow\Doctrine\Bulk\QueryFactory;
use Flow\ETL\Adapter\Doctrine\BulkOperation\Insert;
use Flow\ETL\Adapter\Doctrine\BulkOperation\InsertOrSkipOnConflict;
use Flow\ETL\Adapter\Doctrine\BulkOperation\InsertOrUpdateOnConstraintConflict;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

/**
 * @deprecated please use DbalLoader
 */
final class DbalBulkLoader implements Loader
{
    private Connection $connection;

    private BulkOperation $bulkOperation;

    private int $bulkChunkSize;

    private string $table;

    public function __construct(Connection $connection, BulkOperation $bulkOperation, int $bulkChunkSize, string $table)
    {
        $this->bulkOperation = $bulkOperation;
        $this->bulkChunkSize = $bulkChunkSize;
        $this->table = $table;
        $this->connection = $connection;
    }

    public static function insert(
        Connection $connection,
        int $bulkChunkSize,
        string $table,
        QueryFactory $queryFactory = null
    ) : self {
        return new self(
            $connection,
            new Insert(
                $queryFactory ? new BulkInsert($queryFactory) : BulkInsert::create()
            ),
            $bulkChunkSize,
            $table
        );
    }

    public static function insertOrSkipOnConflict(
        Connection $connection,
        int $bulkChunkSize,
        string $table,
        QueryFactory $queryFactory = null
    ) : self {
        return new self(
            $connection,
            new InsertOrSkipOnConflict(
                $queryFactory ? new BulkInsert($queryFactory) : BulkInsert::create()
            ),
            $bulkChunkSize,
            $table
        );
    }

    public static function insertOrUpdateOnConstraintConflict(
        Connection $connection,
        int $bulkChunkSize,
        string $table,
        string $constraint,
        QueryFactory $queryFactory = null
    ) : self {
        return new self(
            $connection,
            new InsertOrUpdateOnConstraintConflict(
                $queryFactory ? new BulkInsert($queryFactory) : BulkInsert::create(),
                $constraint
            ),
            $bulkChunkSize,
            $table
        );
    }

    public function __serialize() : array
    {
        throw new RuntimeException('DbalBulkLoader is not serializable, please use DbalLoader');
    }

    public function __unserialize(array $data) : void
    {
        throw new RuntimeException('DbalBulkLoader is not serializable, please use DbalLoader');
    }

    public function load(Rows $rows) : void
    {
        foreach ($rows->chunks($this->bulkChunkSize) as $chunk) {
            $this->bulkOperation->execute($this->connection, $this->table, new BulkData($chunk->sortEntries()->toArray()));
        }
    }
}
