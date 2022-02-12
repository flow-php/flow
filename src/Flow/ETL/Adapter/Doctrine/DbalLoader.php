<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\BulkInsert;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final class DbalLoader implements Loader
{
    private string $tableName;

    private int $chunkSize;

    /**
     * @var array<string, mixed>
     */
    private array $connectionParams;

    /**
     * @var array{
     *    do_nothing?: boolean,
     *    constraint?: string,
     *    conflict_columns?: array<string>,
     *    update_columns?: array<string>
     *  }
     */
    private array $insertOptions;

    private ?Connection $connection = null;

    /**
     * @param string $tableName
     * @param int $chunkSize
     * @param array<string, mixed> $connectionParams
     * @param array{
     *  do_nothing?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>
     * } $insertOptions
     */
    public function __construct(
        string $tableName,
        int $chunkSize,
        array $connectionParams,
        array $insertOptions = []
    ) {
        $this->tableName = $tableName;
        $this->chunkSize = $chunkSize;
        $this->connectionParams = $connectionParams;
        $this->insertOptions = $insertOptions;
    }

    /**
     * Since Connection::getParams() is marked as an internal method, please
     * use this constructor with caution.
     *
     * @param Connection $connection
     * @param string $tableName
     * @param int $chunkSize
     * @param array{
     *  do_nothing?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>
     * } $insertOptions
     */
    public static function fromConnection(
        Connection $connection,
        string $tableName,
        int $chunkSize,
        array $insertOptions = []
    ) : self {
        /** @psalm-suppress InternalMethod */
        $loader = new self($tableName, $chunkSize, $connection->getParams(), $insertOptions);
        $loader->connection = $connection;

        return $loader;
    }

    /**
     * @return array{
     *  table_name: string,
     *  chunk_size: int,
     *  connection_params: array<string, mixed>,
     *  insert_options: array{
     *    do_nothing?: boolean,
     *    constraint?: string,
     *    conflict_columns?: array<string>,
     *    update_columns?: array<string>
     *  }
     * }
     */
    public function __serialize() : array
    {
        return [
            'table_name' => $this->tableName,
            'chunk_size' => $this->chunkSize,
            'connection_params' => $this->connectionParams,
            'insert_options' => $this->insertOptions,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{
     *  table_name: string,
     *  chunk_size: int,
     *  connection_params: array<string, mixed>,
     *  insert_options: array{
     *    do_nothing?: boolean,
     *    constraint?: string,
     *    conflict_columns?: array<string>,
     *    update_columns?: array<string>
     *  }
     * } $data
     */
    public function __unserialize(array $data) : void
    {
        $this->tableName = $data['table_name'];
        $this->chunkSize = $data['chunk_size'];
        $this->connectionParams = $data['connection_params'];
        $this->insertOptions = $data['insert_options'];
    }

    public function load(Rows $rows) : void
    {
        foreach ($rows->chunks($this->chunkSize) as $chunk) {
            BulkInsert::create()->insert(
                $this->connection(),
                $this->tableName,
                new BulkData($chunk->sortEntries()->toArray()),
                $this->insertOptions
            );
        }
    }

    private function connection() : Connection
    {
        if ($this->connection === null) {
            /** @psalm-suppress ArgumentTypeCoercion */
            $this->connection = DriverManager::getConnection($this->connectionParams);
        }

        return $this->connection;
    }
}
