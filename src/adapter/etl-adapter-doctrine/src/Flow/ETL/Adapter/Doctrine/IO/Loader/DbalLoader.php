<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Loader;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\InsertOptions;
use Flow\Doctrine\Bulk\UpdateOptions;
use Flow\ETL\Adapter\Doctrine\Values\Schema\TypesMap;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function in_array;
use function strtolower;

/**
 * @import-type Params from DriverManager
 */
final class DbalLoader implements Loader
{
    private ?Bulk $bulk = null;

    private ?Connection $connection = null;

    private ?DbalEncoder $encoder = null;

    private string $operation = 'insert';

    private InsertOptions|UpdateOptions|null $operationOptions = null;

    private ?TypesMap $typesMap = null;

    /**
     * @param array<string, mixed> $connectionParams
     */
    public function __construct(
        private readonly string $tableName,
        private readonly array $connectionParams,
    ) {}

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
    ): self {
        $loader = new self($tableName, $connection->getParams());

        if ($operation !== 'insert') {
            $loader->withOperation($operation);
        }

        if ($operationOptions) {
            $loader->withOperationOptions($operationOptions);
        }

        $loader->connection = $connection;

        return $loader;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return (new self(
            type_string()->assert($data['table_name'] ?? null),
            type_map(type_string(), type_mixed())->assert($data['connection_params'] ?? null),
        ))->withOperation(type_string()->assert($data['operation'] ?? null));
    }

    public function describe(): ?Description
    {
        if ($this->operationOptions !== null || $this->typesMap !== null) {
            return null;
        }

        return new Description(
            self::class,
            [
                'table_name' => $this->tableName,
                'connection_params' => $this->connectionParams,
                'operation' => $this->operation,
            ],
            secret: true,
            extensions: match ($this->connectionParams['driver'] ?? null) {
                'pdo_mysql' => ['pdo', 'pdo_mysql'],
                'pdo_pgsql' => ['pdo', 'pdo_pgsql'],
                'pdo_sqlite' => ['pdo', 'pdo_sqlite'],
                'pdo_sqlsrv' => ['pdo', 'pdo_sqlsrv'],
                'pdo_oci' => ['pdo', 'pdo_oci'],
                'mysqli' => ['mysqli', 'mysqlnd'],
                'pgsql' => ['pgsql'],
                'sqlite3' => ['sqlite3'],
                'oci8' => ['oci8'],
                'ibm_db2' => ['ibm_db2'],
                'sqlsrv' => ['sqlsrv'],
                default => [],
            },
        );
    }

    public function load(Rows $rows, Backend $backend): void
    {
        if ($rows->count() === 0) {
            return;
        }

        // @mago-expect analysis:string-member-selector
        $this->bulk()->{$this->operation}(
            $this->connection(),
            $this->tableName,
            new BulkData($this->encoder()->encode($rows), $this->typesMap()->flowSchemaTypes($rows->schema())),
            $this->operationOptions,
        );
    }

    /**
     * @throws InvalidArgumentException
     */
    public function withOperation(string $operation): self
    {
        if (false === in_array(strtolower($operation), ['update', 'insert', 'delete'], true)) {
            throw new InvalidArgumentException("Operation can be insert, update, or delete, {$operation} given.");
        }

        $this->operation = $operation;

        return $this;
    }

    public function withOperationOptions(InsertOptions|UpdateOptions|null $operationOptions): self
    {
        $this->operationOptions = $operationOptions;

        return $this;
    }

    /**
     * Set custom types map for Flow Type to DBAL Type conversion.
     */
    public function withTypesMap(TypesMap $typesMap): self
    {
        $this->typesMap = $typesMap;

        return $this;
    }

    private function bulk(): Bulk
    {
        if ($this->bulk === null) {
            $this->bulk = Bulk::create();
        }

        return $this->bulk;
    }

    private function encoder(): DbalEncoder
    {
        return $this->encoder ??= new DbalEncoder();
    }

    private function connection(): Connection
    {
        if ($this->connection === null) {
            /** @var Params $connectionParams */
            $connectionParams = $this->connectionParams;
            $this->connection = DriverManager::getConnection($connectionParams);
        }

        return $this->connection;
    }

    private function typesMap(): TypesMap
    {
        return $this->typesMap ??= new TypesMap([]);
    }
}
