<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Benchmark;

use function Flow\ETL\Adapter\Doctrine\{from_dbal_key_set_qb, from_dbal_limit_offset_qb, pagination_key_asc, pagination_key_set, to_dbal_schema_table, to_dbal_table_insert};
use function Flow\ETL\DSL\{config, df, flow_context};
use Doctrine\DBAL\{DriverManager, ParameterType};
use Doctrine\DBAL\Tools\DsnParser;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final class DbalExtractorBench
{
    private const TABLE_NAME = 'benchmark_orders_extractor';

    private \Doctrine\DBAL\Connection $connection;

    public function __construct()
    {
        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new \RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $params = (new DsnParser(['postgresql' => 'pdo_pgsql']))->parse($dsn);

        $this->connection = DriverManager::getConnection($params);

        $this->setupDatabase();
    }

    public function __destruct()
    {
        $schemaManager = $this->connection->createSchemaManager();

        if ($schemaManager->tablesExist([self::TABLE_NAME])) {
            $schemaManager->dropTable(self::TABLE_NAME);
        }

        $this->connection->close();
    }

    public function bench_extract_10k_keyset() : void
    {
        $context = flow_context(config());

        $queryBuilder = $this->connection->createQueryBuilder()
            ->select('*')
            ->from(self::TABLE_NAME);

        foreach (from_dbal_key_set_qb(
            $this->connection,
            $queryBuilder,
            pagination_key_set(pagination_key_asc('index', ParameterType::INTEGER)),
        )->extract($context) as $rows) {
        }
    }

    public function bench_extract_10k_limit_offset() : void
    {
        $context = flow_context(config());

        $queryBuilder = $this->connection->createQueryBuilder()
            ->select('*')
            ->from(self::TABLE_NAME)
            ->orderBy('index', 'ASC');

        foreach (from_dbal_limit_offset_qb(
            $this->connection,
            $queryBuilder,
            page_size: 1000
        )->extract($context) as $rows) {
        }
    }

    private function setupDatabase() : void
    {
        $schemaManager = $this->connection->createSchemaManager();

        if ($schemaManager->tablesExist([self::TABLE_NAME])) {
            $schemaManager->dropTable(self::TABLE_NAME);
        }

        $table = to_dbal_schema_table(FakeStaticOrdersExtractor::schema(), self::TABLE_NAME);
        $table->setPrimaryKey(['index']);
        $schemaManager->createTable($table);

        $extractor = new FakeStaticOrdersExtractor(10_000);

        df()
            ->read($extractor)
            ->write(to_dbal_table_insert($this->connection, self::TABLE_NAME))
            ->run();
    }
}
