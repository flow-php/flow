<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\Benchmarks\Service\Services;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use RuntimeException;
use Throwable;

use function Flow\ETL\Adapter\Doctrine\to_dbal_schema_table;

final class DoctrineConnection
{
    public static function open(): Connection
    {
        $dsn = Services::pgsqlDsn();

        try {
            $connection = DriverManager::getConnection((new DsnParser(['postgresql' => 'pdo_pgsql']))->parse($dsn));
            $connection->executeQuery('SELECT 1');

            return $connection;
        } catch (Throwable $e) {
            throw new RuntimeException(
                'Cannot connect to PostgreSQL (DBAL) at '
                    . $dsn
                    . '. Start the docker compose services (docker compose up -d postgres) or override PGSQL_DATABASE_URL. Original error: '
                    . $e->getMessage(),
                previous: $e,
            );
        }
    }

    /**
     * $keyed adds the primary key on order_id, the read scenarios' keyset: without its index every page scans and
     * sorts the whole table. The write scenarios leave it off - phpbench's warmup inserts the same rows twice per
     * iteration.
     */
    public static function createTable(Connection $connection, string $table, bool $keyed = false): void
    {
        $definition = to_dbal_schema_table((new FakeRandomOrdersExtractor())->schema(), $table);

        if ($keyed) {
            $definition->addPrimaryKeyConstraint(
                PrimaryKeyConstraint::editor()->setUnquotedColumnNames('order_id')->create(),
            );
        }

        $connection->createSchemaManager()->createTable($definition);
    }

    public static function dropTable(Connection $connection, string $table): void
    {
        $schemaManager = $connection->createSchemaManager();

        if ($schemaManager->tablesExist([$table])) {
            $schemaManager->dropTable($table);
        }
    }
}
