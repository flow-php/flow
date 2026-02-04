<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\{Connection, DriverManager, TransactionIsolationLevel};
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Loader, Rows};

final class TransactionalDbalLoader implements Loader
{
    private ?Connection $connection = null;

    private TransactionIsolationLevel|int|null $isolationLevel = null;

    /**
     * @var array<Loader>
     */
    private readonly array $loaders;

    /**
     * @param array<string, mixed> $connectionParams
     * @param Loader ...$loaders
     */
    public function __construct(
        private readonly array $connectionParams,
        Loader ...$loaders,
    ) {
        if (\count($loaders) === 0) {
            throw new InvalidArgumentException('At least one loader must be provided');
        }

        $this->loaders = $loaders;
    }

    /**
     * Since Connection::getParams() is marked as an internal method, please
     * use this constructor with caution.
     */
    public static function fromConnection(
        Connection $connection,
        Loader ...$loaders,
    ) : self {
        $loader = new self($connection->getParams(), ...$loaders);
        $loader->connection = $connection;

        return $loader;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($rows->count() === 0) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            $this->executeInTransaction($this->connection(), $rows, $context);

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (\Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withIsolationLevel(TransactionIsolationLevel|int $level) : self
    {
        $this->isolationLevel = $level;

        return $this;
    }

    private function connection() : Connection
    {
        if ($this->connection === null) {
            /** @phpstan-ignore-next-line */
            $this->connection = DriverManager::getConnection($this->connectionParams);
        }

        return $this->connection;
    }

    private function executeInTransaction(Connection $connection, Rows $rows, FlowContext $context) : void
    {
        $previousIsolationLevel = null;

        if ($this->isolationLevel !== null) {
            $previousIsolationLevel = $connection->getTransactionIsolation();
            /** @phpstan-ignore-next-line */
            $connection->setTransactionIsolation($this->isolationLevel);
        }

        try {
            $connection->beginTransaction();

            try {
                foreach ($this->loaders as $loader) {
                    $loader->load($rows, $context);
                }

                $connection->commit();
            } catch (\Throwable $e) {
                $connection->rollBack();

                throw $e;
            }
        } finally {
            if ($previousIsolationLevel !== null) {
                $connection->setTransactionIsolation($previousIsolationLevel);
            }
        }
    }
}
