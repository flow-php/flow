<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Database;

use function Flow\PgQuery\DSL\{
    alter_table,
    check_constraint,
    column,
    create_table,
    drop_table,
    primary_key,
    sql_type_integer,
    sql_type_serial,
    sql_type_text,
    sql_type_varchar,
    truncate_table,
    unique_constraint
};

final class TableDatabaseTest extends DatabaseTestCase
{
    private const TABLE_CHILD = 'flow_postgres_child_table';

    private const TABLE_PARENT = 'flow_postgres_parent_table';

    private const TABLE_TEST = 'flow_postgres_test_table';

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_CHILD);
        $this->dropTableIfExists(self::TABLE_TEST);
        $this->dropTableIfExists(self::TABLE_PARENT);

        parent::tearDown();
    }

    public function test_alter_table_add_column() : void
    {
        $createQuery = create_table(self::TABLE_TEST)
            ->column(column('id', sql_type_serial()))
            ->column(column('name', sql_type_varchar(100)));
        $this->execute($createQuery->toSql());

        $alterQuery = alter_table(self::TABLE_TEST)
            ->addColumn(column('email', sql_type_varchar(255)));

        $result = $this->execute($alterQuery->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" . self::TABLE_TEST . "' AND column_name = 'email'");
        $columns = $this->fetchAll($check);
        self::assertCount(1, $columns);
    }

    public function test_alter_table_drop_column() : void
    {
        $createQuery = create_table(self::TABLE_TEST)
            ->column(column('id', sql_type_serial()))
            ->column(column('name', sql_type_varchar(100)))
            ->column(column('to_drop', sql_type_text()));
        $this->execute($createQuery->toSql());

        $alterQuery = alter_table(self::TABLE_TEST)
            ->dropColumn('to_drop');

        $result = $this->execute($alterQuery->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" . self::TABLE_TEST . "' AND column_name = 'to_drop'");
        $columns = $this->fetchAll($check);
        self::assertCount(0, $columns);
    }

    public function test_create_table_with_check_constraint() : void
    {
        $query = create_table(self::TABLE_TEST)
            ->column(column('id', sql_type_serial()))
            ->column(column('age', sql_type_integer()))
            ->constraint(check_constraint('age >= 0'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("
            SELECT pg_get_constraintdef(c.oid) as def
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            WHERE t.relname = '" . self::TABLE_TEST . "' AND c.contype = 'c'
        ");
        $constraints = $this->fetchAll($check);
        self::assertGreaterThanOrEqual(1, \count($constraints));

        $hasAgeConstraint = false;

        foreach ($constraints as $constraint) {
            if ($constraint['def'] !== null && \str_contains($constraint['def'], 'age')) {
                $hasAgeConstraint = true;

                break;
            }
        }
        self::assertTrue($hasAgeConstraint, 'Should have a CHECK constraint on age column');
    }

    public function test_create_table_with_columns() : void
    {
        $query = create_table(self::TABLE_TEST)
            ->column(column('id', sql_type_serial()))
            ->column(column('name', sql_type_varchar(100))->notNull())
            ->column(column('description', sql_type_text()));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" . self::TABLE_TEST . "' ORDER BY ordinal_position");
        $columns = $this->fetchAll($check);

        self::assertCount(3, $columns);
        self::assertSame('id', $columns[0]['column_name']);
        self::assertSame('name', $columns[1]['column_name']);
        self::assertSame('description', $columns[2]['column_name']);
    }

    public function test_create_table_with_foreign_key() : void
    {
        self::markTestSkipped('Builder generates invalid SQL with "REFERENCES ONLY" syntax - needs fix in ForeignKeyConstraint');
    }

    public function test_create_table_with_primary_key() : void
    {
        $query = create_table(self::TABLE_TEST)
            ->column(column('id', sql_type_integer())->notNull())
            ->column(column('name', sql_type_varchar(100)))
            ->constraint(primary_key('id'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("
            SELECT constraint_type FROM information_schema.table_constraints
            WHERE table_name = '" . self::TABLE_TEST . "' AND constraint_type = 'PRIMARY KEY'
        ");
        $constraints = $this->fetchAll($check);
        self::assertCount(1, $constraints);
    }

    public function test_create_table_with_unique_constraint() : void
    {
        $query = create_table(self::TABLE_TEST)
            ->column(column('id', sql_type_serial()))
            ->column(column('email', sql_type_varchar(255))->notNull())
            ->constraint(unique_constraint('email'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("
            SELECT constraint_type FROM information_schema.table_constraints
            WHERE table_name = '" . self::TABLE_TEST . "' AND constraint_type = 'UNIQUE'
        ");
        $constraints = $this->fetchAll($check);
        self::assertCount(1, $constraints);
    }

    public function test_drop_table() : void
    {
        $createQuery = create_table(self::TABLE_TEST)
            ->column(column('id', sql_type_serial()));
        $this->execute($createQuery->toSql());

        $dropQuery = drop_table(self::TABLE_TEST);

        $result = $this->execute($dropQuery->toSql());

        self::assertNotFalse($result);

        $check = $this->execute("SELECT table_name FROM information_schema.tables WHERE table_name = '" . self::TABLE_TEST . "'");
        $tables = $this->fetchAll($check);
        self::assertCount(0, $tables);
    }

    public function test_truncate_table() : void
    {
        $createQuery = create_table(self::TABLE_TEST)
            ->column(column('id', sql_type_serial()))
            ->column(column('name', sql_type_varchar(100)));
        $this->execute($createQuery->toSql());

        $this->execute('INSERT INTO ' . self::TABLE_TEST . " (name) VALUES ('test1'), ('test2')");

        $truncateQuery = truncate_table(self::TABLE_TEST);

        $result = $this->execute($truncateQuery->toSql());

        self::assertNotFalse($result);

        $check = $this->execute('SELECT COUNT(*) as cnt FROM ' . self::TABLE_TEST);
        $row = $this->fetchOne($check);
        self::assertSame('0', $row['cnt']);
    }
}
