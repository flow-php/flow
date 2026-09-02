<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\InsertOptions;
use Flow\Doctrine\Bulk\UpdateOptions;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Throwable;

use function in_array;
use function strtolower;

/**
 * @phpstan-import-type Params from DriverManager
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

    public function load(Rows $rows, FlowContext $context): void
    {
        if ($rows->count() === 0) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            // @mago-expect analysis:string-member-selector
            $this->bulk()->{$this->operation}(
                $this->connection(),
                $this->tableName,
                new BulkData(
                    $this->encoder()->encode($context->hydrator()->dehydrate($rows)),
                    $this->typesMap()->flowSchemaTypes($rows->schema()),
                ),
                $this->operationOptions,
            );

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
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
