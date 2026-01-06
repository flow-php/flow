<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Benchmark;

use function Flow\ETL\Adapter\Doctrine\{to_dbal_schema_table, to_dbal_table_insert};
use function Flow\ETL\DSL\flow_context;
use Doctrine\DBAL\{Connection, DriverManager};
use Doctrine\DBAL\Tools\DsnParser;
use Flow\ETL\{FlowContext, Rows};
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use PhpBench\Attributes\{BeforeMethods, Groups};

#[Groups(['loader'])]
final class DbalLoaderBench
{
    private const TABLE_NAME = 'benchmark_orders_loader';

    private Connection $connection;

    private readonly FlowContext $context;

    private Rows $rows;

    public function __construct()
    {
        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new \RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $params = (new DsnParser(['postgresql' => 'pdo_pgsql']))->parse($dsn);

        $this->connection = DriverManager::getConnection($params);
        $this->rows = (new FakeStaticOrdersExtractor(10_000))->toRows();
        $this->context = flow_context();
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
        foreach ($this->rows->chunks(1_000) as $chunk) {
            to_dbal_table_insert($this->connection, self::TABLE_NAME)->load($chunk, $this->context);
        }
    }
}
