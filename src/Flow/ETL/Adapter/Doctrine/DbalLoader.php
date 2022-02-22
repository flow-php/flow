<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\ETL\Exception\InvalidArgumentException;
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
     *    update_columns?: array<string>,
     *    primary_key_columns?: array<string>
     *  }
     */
    private array $operationOptions;

    private ?Connection $connection = null;

    private string $operation;

    /**
     * @param string $tableName
     * @param int $chunkSize
     * @param array<string, mixed> $connectionParams
     * @param array{
     *  do_nothing?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $operationOptions
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        string $tableName,
        int $chunkSize,
        array $connectionParams,
        array $operationOptions = [],
        string $operation = 'insert'
    ) {
        if (false === \in_array(\strtolower($operation), ['update', 'insert'], true)) {
            throw new InvalidArgumentException("Operation can be insert or update, {$operation} given.");
        }

        $this->tableName = $tableName;
        $this->chunkSize = $chunkSize;
        $this->connectionParams = $connectionParams;
        $this->operationOptions = $operationOptions;
        $this->operation = \strtolower($operation);
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
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $operationOptions
     *
     * @throws InvalidArgumentException
     */
    public static function fromConnection(
        Connection $connection,
        string $tableName,
        int $chunkSize,
        array $operationOptions = []
    ) : self {
        /** @psalm-suppress InternalMethod */
        $loader = new self($tableName, $chunkSize, $connection->getParams(), $operationOptions);
        $loader->connection = $connection;

        return $loader;
    }

    /**
     * @return array{
     *  table_name: string,
     *  chunk_size: int,
     *  connection_params: array<string, mixed>,
     *  operation: string,
     *  operation_options: array{
     *    do_nothing?: boolean,
     *    constraint?: string,
     *    conflict_columns?: array<string>,
     *    update_columns?: array<string>,
     *    primary_key_columns?: array<string>
     *  }
     * }
     */
    public function __serialize() : array
    {
        return [
            'table_name' => $this->tableName,
            'chunk_size' => $this->chunkSize,
            'connection_params' => $this->connectionParams,
            'operation' => $this->operation,
            'operation_options' => $this->operationOptions,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{
     *  table_name: string,
     *  chunk_size: int,
     *  connection_params: array<string, mixed>,
     *  operation: string,
     *  operation_options: array{
     *    do_nothing?: boolean,
     *    constraint?: string,
     *    conflict_columns?: array<string>,
     *    update_columns?: array<string>,
     *    primary_key_columns?: array<string>
     *  }
     * } $data
     */
    public function __unserialize(array $data) : void
    {
        $this->tableName = $data['table_name'];
        $this->chunkSize = $data['chunk_size'];
        $this->connectionParams = $data['connection_params'];
        $this->operation = $data['operation'];
        $this->operationOptions = $data['operation_options'];
    }

    public function load(Rows $rows) : void
    {
        foreach ($rows->chunks($this->chunkSize) as $chunk) {
            Bulk::create()->{$this->operation}(
                $this->connection(),
                $this->tableName,
                new BulkData($chunk->sortEntries()->toArray()),
                $this->operationOptions
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
