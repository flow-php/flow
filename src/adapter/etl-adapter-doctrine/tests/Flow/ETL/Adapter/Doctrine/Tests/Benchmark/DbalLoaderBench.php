<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Benchmark;

use function Flow\ETL\Adapter\Doctrine\{to_dbal_schema_table, to_dbal_table_insert};
use function Flow\ETL\DSL\df;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use PhpBench\Attributes\{BeforeMethods, Groups};

#[Groups(['loader'])]
final class DbalLoaderBench
{
    private const TABLE_NAME = 'benchmark_orders_loader';

    private \Doctrine\DBAL\Connection $connection;

    public function __construct()
    {
        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new \RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $params = (new DsnParser(['postgresql' => 'pdo_pgsql']))->parse($dsn);

        $this->connection = DriverManager::getConnection($params);
    }

    public function __destruct()
    {
        $schemaManager = $this->connection->createSchemaManager();

        if ($schemaManager->tablesExist([self::TABLE_NAME])) {
            $schemaManager->dropTable(self::TABLE_NAME);
        }

        $this->connection->close();
    }

    public function setUp() : void
    {
        $schemaManager = $this->connection->createSchemaManager();

        if ($schemaManager->tablesExist([self::TABLE_NAME])) {
            $schemaManager->dropTable(self::TABLE_NAME);
        }

        $table = to_dbal_schema_table(FakeStaticOrdersExtractor::schema(), self::TABLE_NAME);
        $table->setPrimaryKey(['index']);
        $schemaManager->createTable($table);
    }

    #[BeforeMethods('setUp')]
    public function bench_load_10k() : void
    {
        df()
            ->read(new FakeStaticOrdersExtractor(10_000))
            ->write(to_dbal_table_insert($this->connection, self::TABLE_NAME))
            ->run();
    }
}
