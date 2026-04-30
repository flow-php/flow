<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    alter,
    col,
    column,
    column_type_serial,
    create,
    drop,
    gt,
    insert,
    literal,
    primary_key,
    select,
    star,
    table
};

use Flow\PostgreSql\QueryBuilder\Condition\OperatorCondition;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class DomainDatabaseTest extends PostgreSqlTestCase
{
    private const DOMAIN_NAME = 'flow_postgres_email_domain';

    private const DOMAIN_NAME_2 = 'flow_postgres_positive_int_domain';

    private const TABLE_NAME = 'flow_postgres_domain_test';

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_NAME);
        $this->pgsqlContext()->dropDomainIfExists(self::DOMAIN_NAME);
        $this->pgsqlContext()->dropDomainIfExists(self::DOMAIN_NAME_2);

        parent::tearDown();
    }

    public function test_alter_domain_add_constraint() : void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(ColumnType::integer())
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->addConstraint('positive_check', gt(col('value'), literal(0)))
                ->toSql()
        );
    }

    public function test_alter_domain_drop_constraint() : void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(ColumnType::integer())
                ->constraint('positive_check')
                ->check(gt(col('value'), literal(0)))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->dropConstraint('positive_check')
                ->toSql()
        );
    }

    public function test_alter_domain_drop_default() : void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(ColumnType::integer())
                ->default(literal(0))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->dropDefault()
                ->toSql()
        );
    }

    public function test_alter_domain_drop_not_null() : void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(ColumnType::integer())
                ->notNull()
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->dropNotNull()
                ->toSql()
        );
    }

    public function test_alter_domain_set_default() : void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(ColumnType::integer())
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->setDefault(literal(100))
                ->toSql()
        );
    }

    public function test_alter_domain_set_not_null() : void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(ColumnType::integer())
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->setNotNull()
                ->toSql()
        );
    }

    public function test_create_domain() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(ColumnType::varchar(255))
                ->toSql()
        );

        self::assertTrue($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_create_domain_and_use_in_table() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(ColumnType::varchar(255))
                ->constraint('valid_email')
                ->check(new OperatorCondition(col('value'), '~', literal('^[^@]+@[^@]+\\.[^@]+$')))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', column_type_serial()))
                ->column(column('email', ColumnType::custom(self::DOMAIN_NAME))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_NAME)
                ->columns('email')
                ->values(literal('test@example.com'))
                ->toSql()
        );

        $row = $this->pgsqlContext()->client()->fetchSingle(
            select(star())->from(table(self::TABLE_NAME))->toSql()
        );

        self::assertSame('test@example.com', $row['email']);
    }

    public function test_create_domain_with_check_constraint() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(ColumnType::varchar(255))
                ->constraint('valid_email')
                ->check(new OperatorCondition(col('value'), '~', literal('^[^@]+@[^@]+\\.[^@]+$')))
                ->toSql()
        );

        self::assertTrue($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_create_domain_with_default() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(ColumnType::integer())
                ->default(literal(0))
                ->toSql()
        );

        self::assertTrue($this->domainExists(self::DOMAIN_NAME_2));
    }

    public function test_create_domain_with_not_null() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(ColumnType::varchar(255))
                ->notNull()
                ->toSql()
        );

        self::assertTrue($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_drop_domain() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(ColumnType::varchar(255))
                ->toSql()
        );

        self::assertTrue($this->domainExists(self::DOMAIN_NAME));

        $this->pgsqlContext()->client()->execute(
            drop()->domain(self::DOMAIN_NAME)->toSql()
        );

        self::assertFalse($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_drop_domain_cascade() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(ColumnType::varchar(255))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', column_type_serial()))
                ->column(column('email', ColumnType::custom(self::DOMAIN_NAME)))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            drop()->domain(self::DOMAIN_NAME)->cascade()->toSql()
        );

        self::assertFalse($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_drop_domain_if_exists() : void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(
            drop()->domain(self::DOMAIN_NAME)->ifExists()->toSql()
        );
    }

    private function domainExists(string $name) : bool
    {
        $row = $this->pgsqlContext()->client()->fetchSingle(
            "SELECT EXISTS(
                    SELECT 1 FROM pg_type t
                    JOIN pg_namespace n ON t.typnamespace = n.oid
                    WHERE t.typname = '{$name}'
                    AND t.typtype = 'd'
                ) AS domain_exists"
        );

        return $row['domain_exists'] === true;
    }
}
