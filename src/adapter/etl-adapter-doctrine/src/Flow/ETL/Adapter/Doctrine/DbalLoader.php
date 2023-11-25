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
final class DbalLoader implements Loader, Loader\BatchLoader
{
    private ?Connection $connection = null;

    private string $operation;

    /**
     * @param array<string, mixed> $connectionParams
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $operationOptions
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private string $tableName,
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
     * @param array{
     *  skip_conflicts?: boolean,
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
        array $operationOptions = [],
        string $operation = 'insert'
    ) : self {
        /** @psalm-suppress InternalMethod */
        $loader = new self($tableName, $connection->getParams(), $operationOptions, $operation);
        $loader->connection = $connection;

        return $loader;
    }

    public function __serialize() : array
    {
        return [
            'table_name' => $this->tableName,
            'connection_params' => $this->connectionParams,
            'operation' => $this->operation,
            'operation_options' => $this->operationOptions,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->tableName = $data['table_name'];
        $this->connectionParams = $data['connection_params'];
        $this->operation = $data['operation'];
        $this->operationOptions = $data['operation_options'];
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        Bulk::create()->{$this->operation}(
            $this->connection(),
            $this->tableName,
            new BulkData($rows->sortEntries()->toArray()),
            $this->operationOptions
        );
    }

    private function connection() : Connection
    {
        if ($this->connection === null) {
            /**
             * @psalm-suppress ArgumentTypeCoercion
             */
            $this->connection = DriverManager::getConnection($this->connectionParams);
        }

        return $this->connection;
    }
}
