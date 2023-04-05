<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{
 *  table_name: string,
 *  chunk_size: int,
 *  connection_params: array<string, mixed>,
 *  operation: string,
 *  operation_options: array{
 *    skip_conflicts?: boolean,
 *    constraint?: string,
 *    conflict_columns?: array<string>,
 *    update_columns?: array<string>,
 *    primary_key_columns?: array<string>
 *  }
 * }>
 */
final class DbalLoader implements Loader
{
    private ?Connection $connection = null;

    private string $operation;

    /**
     * @param string $tableName
     * @param int $chunkSize
     * @param array<string, mixed> $connectionParams
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $operationOptions
     * @param string $operation
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private string $tableName,
        private int $chunkSize,
        private array $connectionParams,
        private array $operationOptions = [],
        string $operation = 'insert'
    ) {
        if (false === \in_array(\strtolower($operation), ['update', 'insert'], true)) {
            throw new InvalidArgumentException("Operation can be insert or update, {$operation} given.");
        }
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
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $operationOptions
     * @param string $operation
     *
     * @throws InvalidArgumentException
     */
    public static function fromConnection(
        Connection $connection,
        string $tableName,
        int $chunkSize,
        array $operationOptions = [],
        string $operation = 'insert'
    ) : self {
        /** @psalm-suppress InternalMethod */
        $loader = new self($tableName, $chunkSize, $connection->getParams(), $operationOptions, $operation);
        $loader->connection = $connection;

        return $loader;
    }

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

    public function __unserialize(array $data) : void
    {
        $this->tableName = $data['table_name'];
        $this->chunkSize = $data['chunk_size'];
        $this->connectionParams = $data['connection_params'];
        $this->operation = $data['operation'];
        $this->operationOptions = $data['operation_options'];
    }

    public function load(Rows $rows, FlowContext $context) : void
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
            /**
             * @psalm-suppress ArgumentTypeCoercion
             *
             * @phpstan-ignore-next-line
             */
            $this->connection = DriverManager::getConnection($this->connectionParams);
        }

        return $this->connection;
    }
}
