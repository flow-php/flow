<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;
use Throwable;

use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\ETL\Adapter\PostgreSql\to_pgsql_table;
use function Flow\ETL\Adapter\PostgreSql\to_pgsql_transaction;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final class TransactionalPostgreSqlLoaderIntegrationTest extends IntegrationTestCase
{
    private string $primaryTable = 'flow_pgsql_tx_primary';

    private string $mirrorTable = 'flow_pgsql_tx_mirror';

    protected function setUp(): void
    {
        parent::setUp();

        foreach ([$this->primaryTable, $this->mirrorTable] as $tableName) {
            $this->client->execute(
                create()
                    ->table($tableName)
                    ->column(column('id', column_type_integer())->primaryKey())
                    ->column(column('name', column_type_text())),
            );
        }
    }

    public function test_commits_every_loader_in_a_single_transaction(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ]))
            ->write(to_pgsql_transaction(
                $this->client,
                to_pgsql_table($this->client, $this->primaryTable),
                to_pgsql_table($this->client, $this->mirrorTable),
            ))
            ->run();

        $expected = [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']];

        static::assertSame($expected, $this->fetchAll($this->primaryTable));
        static::assertSame($expected, $this->fetchAll($this->mirrorTable));
    }

    public function test_rolls_back_all_loaders_when_one_fails(): void
    {
        df()
            ->read(from_array([['id' => 1, 'name' => 'Existing']]))
            ->write(to_pgsql_table($this->client, $this->mirrorTable))
            ->run();

        try {
            df()
                ->read(from_array([
                    ['id' => 1, 'name' => 'Alice'],
                    ['id' => 2, 'name' => 'Bob'],
                ]))
                ->write(to_pgsql_transaction(
                    $this->client,
                    to_pgsql_table($this->client, $this->primaryTable),
                    to_pgsql_table($this->client, $this->mirrorTable),
                ))
                ->run();

            static::fail('Expected a primary key violation to be thrown');
        } catch (Throwable) {
            // id=1 already exists in the mirror table; the whole batch must roll back
        }

        static::assertSame([], $this->fetchAll($this->primaryTable));
        static::assertSame([['id' => 1, 'name' => 'Existing']], $this->fetchAll($this->mirrorTable));
    }

    /**
     * @return array<int, array<array-key, mixed>>
     */
    private function fetchAll(string $tableName): array
    {
        return df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($tableName))->orderBy(asc(col('id'))),
            ))
            ->fetch()
            ->toArray();
    }
}
