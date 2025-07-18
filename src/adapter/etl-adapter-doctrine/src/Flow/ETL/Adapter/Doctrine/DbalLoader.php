<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\{Connection, DriverManager};
use Doctrine\DBAL\Types\Type;
use Flow\Doctrine\Bulk\{Bulk, BulkData, InsertOptions, UpdateOptions};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Loader, Rows};

final class DbalLoader implements Loader
{
    private ?Bulk $bulk = null;

    /**
     * @var null|array<string, Type>
     */
    private ?array $columnTypes = null;

    private ?Connection $connection = null;

    private string $operation = 'insert';

    private InsertOptions|UpdateOptions|null $operationOptions = null;

    private ?DbalTypesDetector $typesDetector = null;

    /**
     * @param array<string, mixed> $connectionParams
     */
    public function __construct(
        private readonly string $tableName,
        private readonly array $connectionParams,
    ) {
    }

    /**
     * Since Connection::getParams() is marked as an internal method, please
     * use this constructor with caution.
     *
     * @throws InvalidArgumentException
     */
    public static function fromConnection(
        Connection $connection,
        string $tableName,
        InsertOptions|UpdateOptions|null $operationOptions = null,
        string $operation = 'insert',
    ) : self {
        $loader = (new self($tableName, $connection->getParams()));

        if ($operation !== 'insert') {
            $loader->withOperation($operation);
        }

        if ($operationOptions) {
            $loader->withOperationOptions($operationOptions);
        }

        $loader->connection = $connection;

        return $loader;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $normalizedData = (new RowsNormalizer())->normalize($rows->sortEntries());

        $this->bulk()->{$this->operation}(
            $this->connection(),
            $this->tableName,
            new BulkData($normalizedData, $this->typesDetector()->convert($rows->schema(), $this->columnTypes ?? [])),
            $this->operationOptions
        );
    }

    /**
     * Override types taken from Flow Schema with explicitly provided DBAL types.
     *
     * @param array<string, Type> $types Column name => DBAL Type instance
     */
    public function withColumnTypes(array $types) : self
    {
        $this->columnTypes = $types;

        return $this;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function withOperation(string $operation) : self
    {
        if (false === \in_array(\strtolower($operation), ['update', 'insert', 'delete'], true)) {
            throw new InvalidArgumentException("Operation can be insert, update, or delete, {$operation} given.");
        }

        $this->operation = $operation;

        return $this;
    }

    public function withOperationOptions(InsertOptions|UpdateOptions|null $operationOptions) : self
    {
        $this->operationOptions = $operationOptions;

        return $this;
    }

    /**
     * Set custom SchemaToTypesConverter with custom TypesMap.
     */
    public function withTypesDetector(DbalTypesDetector $detector) : self
    {
        $this->typesDetector = $detector;

        return $this;
    }

    private function bulk() : Bulk
    {
        if ($this->bulk === null) {
            $this->bulk = Bulk::create();
        }

        return $this->bulk;
    }

    private function connection() : Connection
    {
        if ($this->connection === null) {
            /** @phpstan-ignore-next-line */
            $this->connection = DriverManager::getConnection($this->connectionParams);
        }

        return $this->connection;
    }

    private function typesDetector() : DbalTypesDetector
    {
        if ($this->typesDetector !== null) {
            return $this->typesDetector;
        }

        $this->typesDetector = new DbalTypesDetector();

        return $this->typesDetector;
    }
}
