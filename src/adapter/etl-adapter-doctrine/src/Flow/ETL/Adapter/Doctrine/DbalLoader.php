<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\{Connection, DriverManager};
use Flow\Doctrine\Bulk\{Bulk, BulkData};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Loader, Rows};

final class DbalLoader implements Loader
{
    private ?Connection $connection = null;

    private string $operation = 'insert';

    private array $operationOptions = [];

    /**
     * @param array<string, mixed> $connectionParams
     */
    public function __construct(
        private string $tableName,
        private readonly array $connectionParams,
    ) {
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
        string $operation = 'insert',
    ) : self {
        $loader = (new self($tableName, $connection->getParams()));

        if ($operation !== 'insert') {
            $loader->withOperation($operation);
        }

        if ($operationOptions !== []) {
            $loader->withOperationOptions($operationOptions);
        }

        $loader->connection = $connection;

        return $loader;
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

    /**
     * @throws InvalidArgumentException
     */
    public function withOperation(string $operation) : self
    {
        if (false === \in_array(\strtolower($operation), ['update', 'insert'], true)) {
            throw new InvalidArgumentException("Operation can be insert or update, {$operation} given.");
        }

        $this->operation = $operation;

        return $this;
    }

    /**
     * @param array{
     *   skip_conflicts?: boolean,
     *   constraint?: string,
     *   conflict_columns?: array<string>,
     *   update_columns?: array<string>,
     *   primary_key_columns?: array<string>
     *  } $operationOptions
     */
    public function withOperationOptions(array $operationOptions) : self
    {
        $this->operationOptions = $operationOptions;

        return $this;
    }

    private function connection() : Connection
    {
        if ($this->connection === null) {
            $this->connection = DriverManager::getConnection($this->connectionParams);
        }

        return $this->connection;
    }
}
