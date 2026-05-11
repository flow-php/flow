<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Trigger\TriggerEvent as BuilderTriggerEvent;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\ne;
use function Flow\PostgreSql\DSL\schema_trigger;

final class PgCatalogTriggerWhenConditionNormalizationTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_trigger_when_norm_test';

    protected function setUp(): void
    {
        parent::setUp();

        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);

        parent::tearDown();
    }

    public function test_trigger_when_condition_round_trip_with_implicit_cast(): void
    {
        $s = self::SCHEMA;

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->function("{$s}.trg_noop_fn")
                    ->arguments()
                    ->returns(ColumnType::custom('trigger'))
                    ->language('plpgsql')
                    ->as('BEGIN RETURN NEW; END;')
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table('orders', $s)
                    ->column(column('id', column_type_integer())->notNull())
                    ->column(column('status', column_type_varchar(16))->notNull())
                    ->column(column('value', column_type_integer())->notNull())
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->trigger('trg_orders_when')
                    ->before(BuilderTriggerEvent::UPDATE)
                    ->on("{$s}.orders")
                    ->forEachRow()
                    ->when(ne(col('value', 'new'), col('value', 'old')))
                    ->execute("{$s}.trg_noop_fn")
                    ->toSql(),
            );

        $expected = schema_trigger(
            name: 'trg_orders_when',
            tableName: 'orders',
            timing: TriggerTiming::BEFORE,
            events: [TriggerEvent::UPDATE],
            functionName: 'trg_noop_fn',
            forEachRow: true,
            whenCondition: 'new.value <> old.value',
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [$s])->get()->get($s)->table('orders');

        static::assertCount(1, $table->triggers);

        $dbTrigger = $table->triggers[0];

        static::assertSame('trg_orders_when', $dbTrigger->name);
        static::assertNotNull($dbTrigger->whenCondition, 'whenCondition must be populated from pg_trigger.tgqual');
        static::assertTrue(
            $expected->isEqualStructure($dbTrigger),
            \sprintf(
                'Expected trigger whenCondition to be structurally equal.%sExpected: %s%sActual:   %s',
                \PHP_EOL,
                $expected->whenCondition ?? '<null>',
                \PHP_EOL,
                $dbTrigger->whenCondition ?? '<null>',
            ),
        );
    }
}
