<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function array_filter;
use function count;
use function Flow\PostgreSql\DSL\agg_count;
use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\check_constraint;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\foreign_key;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\ge;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\truncate_table;
use function Flow\PostgreSql\DSL\unique_constraint;
use function is_string;
use function str_contains;

final class TableDatabaseTest extends PostgreSqlTestCase
{
    private const TABLE_CHILD = 'flow_postgres_child_table';

    private const TABLE_PARENT = 'flow_postgres_parent_table';

    private const TABLE_TEST = 'flow_postgres_test_table';

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_CHILD);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_TEST);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_PARENT);

        parent::tearDown();
    }

    public function test_alter_table_add_column(): void
    {
        $createQuery = create()
            ->table(self::TABLE_TEST)
            ->column(column('id', column_type_serial()))
            ->column(column('name', column_type_varchar(100)));
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $alterQuery = alter()->table(self::TABLE_TEST)->addColumn(column('email', column_type_varchar(255)));

        $this->pgsqlContext()->client()->execute($alterQuery->toSql());

        $columns = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(
                select(col('column_name'))
                    ->from(table('information_schema.columns'))
                    ->where(and_(
                        eq(col('table_name'), literal(self::TABLE_TEST)),
                        eq(col('column_name'), literal('email')),
                    ))
                    ->toSql(),
            );
        static::assertCount(1, $columns);
    }

    public function test_alter_table_drop_column(): void
    {
        $createQuery = create()
            ->table(self::TABLE_TEST)
            ->column(column('id', column_type_serial()))
            ->column(column('name', column_type_varchar(100)))
            ->column(column('to_drop', column_type_text()));
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $alterQuery = alter()->table(self::TABLE_TEST)->dropColumn('to_drop');

        $this->pgsqlContext()->client()->execute($alterQuery->toSql());

        $columns = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(
                select(col('column_name'))
                    ->from(table('information_schema.columns'))
                    ->where(and_(
                        eq(col('table_name'), literal(self::TABLE_TEST)),
                        eq(col('column_name'), literal('to_drop')),
                    ))
                    ->toSql(),
            );
        static::assertCount(0, $columns);
    }

    public function test_create_table_with_check_constraint(): void
    {
        $query = create()
            ->table(self::TABLE_TEST)
            ->column(column('id', column_type_serial()))
            ->column(column('age', column_type_integer()))
            ->constraint(check_constraint(ge(col('age'), literal(0))));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $constraints = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(
                select(func('pg_get_constraintdef', [col('oid', 'c')])->as('def'))
                    ->from(table('pg_constraint')->as('c'))
                    ->join(table('pg_class')->as('t'), eq(col('conrelid', 'c'), col('oid', 't')))
                    ->where(and_(
                        eq(col('relname', 't'), literal(self::TABLE_TEST)),
                        eq(col('contype', 'c'), literal('c')),
                    ))
                    ->toSql(),
            );
        static::assertGreaterThanOrEqual(1, count($constraints));

        $matchingConstraints = array_filter(
            $constraints,
            static fn(array $constraint): bool => (
                is_string($constraint['def'] ?? null) && str_contains($constraint['def'], 'age')
            ),
        );
        static::assertNotEmpty($matchingConstraints, 'Should have a CHECK constraint on age column');
    }

    public function test_create_table_with_columns(): void
    {
        $query = create()
            ->table(self::TABLE_TEST)
            ->column(column('id', column_type_serial()))
            ->column(column('name', column_type_varchar(100))->notNull())
            ->column(column('description', column_type_text()));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $columns = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(
                select(col('column_name'))
                    ->from(table('information_schema.columns'))
                    ->where(eq(col('table_name'), literal(self::TABLE_TEST)))
                    ->orderBy(asc(col('ordinal_position')))
                    ->toSql(),
            );

        static::assertCount(3, $columns);
        static::assertSame('id', $columns[0]['column_name']);
        static::assertSame('name', $columns[1]['column_name']);
        static::assertSame('description', $columns[2]['column_name']);
    }

    public function test_create_table_with_foreign_key(): void
    {
        $parentQuery = create()
            ->table(self::TABLE_PARENT)
            ->column(column('id', column_type_serial()))
            ->column(column('name', column_type_varchar(100)))
            ->constraint(primary_key('id'));
        $this->pgsqlContext()->client()->execute($parentQuery->toSql());

        $childQuery = create()
            ->table(self::TABLE_CHILD)
            ->column(column('id', column_type_serial()))
            ->column(column('parent_id', column_type_integer())->notNull())
            ->constraint(foreign_key(['parent_id'], self::TABLE_PARENT, ['id']));

        $this->pgsqlContext()->client()->execute($childQuery->toSql());

        $constraints = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(
                select(col('constraint_type', 'tc'), col('table_name', 'ccu')->as('foreign_table_name'))
                    ->from(table('information_schema.table_constraints')->as('tc'))
                    ->join(
                        table('information_schema.constraint_column_usage')->as('ccu'),
                        eq(col('constraint_name', 'tc'), col('constraint_name', 'ccu')),
                    )
                    ->where(and_(
                        eq(col('table_name', 'tc'), literal(self::TABLE_CHILD)),
                        eq(col('constraint_type', 'tc'), literal('FOREIGN KEY')),
                    ))
                    ->toSql(),
            );
        static::assertCount(1, $constraints);
        static::assertSame(self::TABLE_PARENT, $constraints[0]['foreign_table_name']);
    }

    public function test_create_table_with_primary_key(): void
    {
        $query = create()
            ->table(self::TABLE_TEST)
            ->column(column('id', column_type_integer())->notNull())
            ->column(column('name', column_type_varchar(100)))
            ->constraint(primary_key('id'));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $constraints = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(
                select(col('constraint_type'))
                    ->from(table('information_schema.table_constraints'))
                    ->where(and_(
                        eq(col('table_name'), literal(self::TABLE_TEST)),
                        eq(col('constraint_type'), literal('PRIMARY KEY')),
                    ))
                    ->toSql(),
            );
        static::assertCount(1, $constraints);
    }

    public function test_create_table_with_unique_constraint(): void
    {
        $query = create()
            ->table(self::TABLE_TEST)
            ->column(column('id', column_type_serial()))
            ->column(column('email', column_type_varchar(255))->notNull())
            ->constraint(unique_constraint('email'));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $constraints = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(
                select(col('constraint_type'))
                    ->from(table('information_schema.table_constraints'))
                    ->where(and_(
                        eq(col('table_name'), literal(self::TABLE_TEST)),
                        eq(col('constraint_type'), literal('UNIQUE')),
                    ))
                    ->toSql(),
            );
        static::assertCount(1, $constraints);
    }

    public function test_drop_table(): void
    {
        $createQuery = create()->table(self::TABLE_TEST)->column(column('id', column_type_serial()));
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $dropQuery = drop()->table(self::TABLE_TEST);

        $this->pgsqlContext()->client()->execute($dropQuery->toSql());

        $tables = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(
                select(col('table_name'))
                    ->from(table('information_schema.tables'))
                    ->where(eq(col('table_name'), literal(self::TABLE_TEST)))
                    ->toSql(),
            );
        static::assertCount(0, $tables);
    }

    public function test_truncate_table(): void
    {
        $createQuery = create()
            ->table(self::TABLE_TEST)
            ->column(column('id', column_type_serial()))
            ->column(column('name', column_type_varchar(100)));
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_TEST)
                    ->columns('name')
                    ->values(literal('test1'))
                    ->values(literal('test2'))
                    ->toSql(),
            );

        $truncateQuery = truncate_table(self::TABLE_TEST);

        $this->pgsqlContext()->client()->execute($truncateQuery->toSql());

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(select(agg_count()->as('cnt'))->from(table(self::TABLE_TEST))->toSql());
        static::assertSame(0, $row['cnt']);
    }
}
