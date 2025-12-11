<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Database;

use function Flow\PgQuery\DSL\{
    alter,
    column,
    create,
    drop,
    insert,
    literal,
    primary_key,
    select,
    sql_type_serial,
    star,
    table
};

use Flow\PgQuery\QueryBuilder\Schema\DataType;

final class DomainDatabaseTest extends DatabaseTestCase
{
    private const DOMAIN_NAME = 'flow_postgres_email_domain';

    private const DOMAIN_NAME_2 = 'flow_postgres_positive_int_domain';

    private const TABLE_NAME = 'flow_postgres_domain_test';

    protected function tearDown() : void
    {
        $this->dropTableIfExists(self::TABLE_NAME);
        $this->dropDomainIfExists(self::DOMAIN_NAME);
        $this->dropDomainIfExists(self::DOMAIN_NAME_2);

        parent::tearDown();
    }

    public function test_alter_domain_add_constraint() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(DataType::integer())
                ->toSql()
        );

        $result = $this->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->addConstraint('positive_check', 'VALUE > 0')
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_alter_domain_drop_constraint() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(DataType::integer())
                ->constraint('positive_check')
                ->check('VALUE > 0')
                ->toSql()
        );

        $result = $this->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->dropConstraint('positive_check')
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_alter_domain_drop_default() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(DataType::integer())
                ->default('0')
                ->toSql()
        );

        $result = $this->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->dropDefault()
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_alter_domain_drop_not_null() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(DataType::integer())
                ->notNull()
                ->toSql()
        );

        $result = $this->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->dropNotNull()
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_alter_domain_set_default() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(DataType::integer())
                ->toSql()
        );

        $result = $this->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->setDefault('100')
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_alter_domain_set_not_null() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(DataType::integer())
                ->toSql()
        );

        $result = $this->execute(
            alter()->domain(self::DOMAIN_NAME_2)
                ->setNotNull()
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_create_domain() : void
    {
        $result = $this->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(DataType::varchar(255))
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_create_domain_and_use_in_table() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(DataType::varchar(255))
                ->constraint('valid_email')
                ->check("VALUE ~ '^[^@]+@[^@]+\\.[^@]+$'")
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', sql_type_serial()))
                ->column(column('email', DataType::custom(self::DOMAIN_NAME))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_NAME)
                ->columns('email')
                ->values(literal('test@example.com'))
                ->toSql()
        );

        $row = $this->fetchOne(
            $this->execute(
                select(star())->from(table(self::TABLE_NAME))->toSql()
            )
        );

        self::assertSame('test@example.com', $row['email']);
    }

    public function test_create_domain_with_check_constraint() : void
    {
        $result = $this->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(DataType::varchar(255))
                ->constraint('valid_email')
                ->check("VALUE ~ '^[^@]+@[^@]+\\.[^@]+$'")
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_create_domain_with_default() : void
    {
        $result = $this->execute(
            create()->domain(self::DOMAIN_NAME_2)
                ->as(DataType::integer())
                ->default('0')
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->domainExists(self::DOMAIN_NAME_2));
    }

    public function test_create_domain_with_not_null() : void
    {
        $result = $this->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(DataType::varchar(255))
                ->notNull()
                ->toSql()
        );

        self::assertNotFalse($result);
        self::assertTrue($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_drop_domain() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(DataType::varchar(255))
                ->toSql()
        );

        self::assertTrue($this->domainExists(self::DOMAIN_NAME));

        $result = $this->execute(
            drop()->domain(self::DOMAIN_NAME)->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_drop_domain_cascade() : void
    {
        $this->execute(
            create()->domain(self::DOMAIN_NAME)
                ->as(DataType::varchar(255))
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', sql_type_serial()))
                ->column(column('email', DataType::custom(self::DOMAIN_NAME)))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $result = $this->execute(
            drop()->domain(self::DOMAIN_NAME)->cascade()->toSql()
        );

        self::assertNotFalse($result);
        self::assertFalse($this->domainExists(self::DOMAIN_NAME));
    }

    public function test_drop_domain_if_exists() : void
    {
        $result = $this->execute(
            drop()->domain(self::DOMAIN_NAME)->ifExists()->toSql()
        );

        self::assertNotFalse($result);
    }

    protected function domainExists(string $name) : bool
    {
        $row = $this->fetchOne(
            $this->execute(
                "SELECT EXISTS(
                    SELECT 1 FROM pg_type t
                    JOIN pg_namespace n ON t.typnamespace = n.oid
                    WHERE t.typname = '{$name}'
                    AND t.typtype = 'd'
                ) AS domain_exists"
            )
        );

        return $row['domain_exists'] === 't';
    }

    protected function dropDomainIfExists(string $name) : void
    {
        $this->execute("DROP DOMAIN IF EXISTS {$name} CASCADE");
    }
}
