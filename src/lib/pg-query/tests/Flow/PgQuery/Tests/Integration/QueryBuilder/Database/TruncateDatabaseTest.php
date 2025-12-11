<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Database;

use function Flow\PgQuery\DSL\{
    col,
    column,
    create,
    desc,
    insert,
    literal,
    primary_key,
    select,
    sql_type_serial,
    sql_type_varchar,
    star,
    table,
    truncate_table
};

final class TruncateDatabaseTest extends DatabaseTestCase
{
    private const TABLE_ONE = 'flow_postgres_truncate_one';

    private const TABLE_TWO = 'flow_postgres_truncate_two';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_ONE)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_TWO)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_ONE)
                ->columns('name')
                ->values(literal('Alice'))
                ->values(literal('Bob'))
                ->values(literal('Charlie'))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_TWO)
                ->columns('name')
                ->values(literal('Dave'))
                ->values(literal('Eve'))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_TWO);
        $this->dropTableIfExists(self::TABLE_ONE);

        parent::tearDown();
    }

    public function test_truncate_cascade() : void
    {
        $result = $this->execute(
            truncate_table(self::TABLE_ONE)->cascade()->toSql()
        );

        self::assertNotFalse($result);

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_ONE))->toSql()
            )
        );
        self::assertCount(0, $rows);
    }

    public function test_truncate_continue_identity() : void
    {
        $lastRow = $this->fetchOne(
            $this->execute(
                select(col('id'))->from(table(self::TABLE_ONE))->orderBy(desc(col('id')))->limit(1)->toSql()
            )
        );
        $lastId = (int) $lastRow['id'];

        $this->execute(
            truncate_table(self::TABLE_ONE)->continueIdentity()->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_ONE)
                ->columns('name')
                ->values(literal('NewRow'))
                ->toSql()
        );

        $row = $this->fetchOne(
            $this->execute(
                select(star())->from(table(self::TABLE_ONE))->toSql()
            )
        );

        self::assertSame((string) ($lastId + 1), $row['id']);
    }

    public function test_truncate_multiple_tables() : void
    {
        $result = $this->execute(
            truncate_table(self::TABLE_ONE, self::TABLE_TWO)->toSql()
        );

        self::assertNotFalse($result);

        $rowsOne = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_ONE))->toSql()
            )
        );
        self::assertCount(0, $rowsOne);

        $rowsTwo = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TWO))->toSql()
            )
        );
        self::assertCount(0, $rowsTwo);
    }

    public function test_truncate_restart_identity() : void
    {
        $this->execute(
            truncate_table(self::TABLE_ONE)->restartIdentity()->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_ONE)
                ->columns('name')
                ->values(literal('NewRow'))
                ->toSql()
        );

        $row = $this->fetchOne(
            $this->execute(
                select(star())->from(table(self::TABLE_ONE))->toSql()
            )
        );

        self::assertSame('1', $row['id']);
        self::assertSame('NewRow', $row['name']);
    }

    public function test_truncate_restrict() : void
    {
        $result = $this->execute(
            truncate_table(self::TABLE_ONE)->restrict()->toSql()
        );

        self::assertNotFalse($result);

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_ONE))->toSql()
            )
        );
        self::assertCount(0, $rows);
    }

    public function test_truncate_single_table() : void
    {
        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_ONE))->toSql()
            )
        );
        self::assertCount(3, $rows);

        $result = $this->execute(
            truncate_table(self::TABLE_ONE)->toSql()
        );

        self::assertNotFalse($result);

        $rows = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_ONE))->toSql()
            )
        );
        self::assertCount(0, $rows);

        $rowsTwo = $this->fetchAll(
            $this->execute(
                select(star())->from(table(self::TABLE_TWO))->toSql()
            )
        );
        self::assertCount(2, $rowsTwo);
    }

    public function test_truncate_with_restart_identity_and_cascade() : void
    {
        $result = $this->execute(
            truncate_table(self::TABLE_ONE, self::TABLE_TWO)
                ->restartIdentity()
                ->cascade()
                ->toSql()
        );

        self::assertNotFalse($result);

        $this->execute(
            insert()
                ->into(self::TABLE_ONE)
                ->columns('name')
                ->values(literal('AfterTruncate'))
                ->toSql()
        );

        $row = $this->fetchOne(
            $this->execute(
                select(star())->from(table(self::TABLE_ONE))->toSql()
            )
        );

        self::assertSame('1', $row['id']);
    }
}
