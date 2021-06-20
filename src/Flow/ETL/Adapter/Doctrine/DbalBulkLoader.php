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
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final class DbalBulkLoader implements Loader
{
    private BulkOperation $bulkOperation;

    private int $bulkChunkSize;

    private string $table;

    public function __construct(BulkOperation $bulkOperation, int $bulkChunkSize, string $table)
    {
        $this->bulkOperation = $bulkOperation;
        $this->bulkChunkSize = $bulkChunkSize;
        $this->table = $table;
    }

    public static function insert(
        Connection $connection,
        int $bulkChunkSize,
        string $table,
        QueryFactory $queryFactory = null
    ) : self {
        return new self(
            new Insert(
                $queryFactory ? new BulkInsert($connection, $queryFactory) : BulkInsert::create($connection)
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
            new InsertOrSkipOnConflict(
                $queryFactory ? new BulkInsert($connection, $queryFactory) : BulkInsert::create($connection)
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
            new InsertOrUpdateOnConstraintConflict(
                $queryFactory ? new BulkInsert($connection, $queryFactory) : BulkInsert::create($connection),
                $constraint
            ),
            $bulkChunkSize,
            $table
        );
    }

    public function load(Rows $rows) : void
    {
        foreach ($rows->chunks($this->bulkChunkSize) as $chunk) {
            $this->bulkOperation->execute($this->table, new BulkData($chunk->sortEntries()->toArray()));
        }
    }
}
