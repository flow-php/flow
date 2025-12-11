<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder;

use function Flow\PostgreSql\DSL\{alter, create, drop};

use Flow\PostgreSql\QueryBuilder\Schema\DataType;

final class DomainBuilderTest extends PGQueryTestCase
{
    public function test_alter_domain_add_constraint() : void
    {
        $builder = alter()->domain('email')
            ->addConstraint('valid_email', "VALUE ~ '^.+@.+$'");

        $this->assertAlterDomainQuery(
            $builder,
            "ALTER DOMAIN email ADD CONSTRAINT valid_email CHECK (value ~ '^.+@.+$')"
        );
    }

    public function test_alter_domain_drop_constraint() : void
    {
        $builder = alter()->domain('email')
            ->dropConstraint('valid_email');

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email DROP CONSTRAINT valid_email'
        );
    }

    public function test_alter_domain_drop_constraint_cascade() : void
    {
        $builder = alter()->domain('email')
            ->dropConstraint('valid_email')
            ->cascade();

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email DROP CONSTRAINT valid_email CASCADE'
        );
    }

    public function test_alter_domain_drop_default() : void
    {
        $builder = alter()->domain('email')
            ->dropDefault();

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email DROP DEFAULT'
        );
    }

    public function test_alter_domain_drop_not_null() : void
    {
        $builder = alter()->domain('email')
            ->dropNotNull();

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email DROP NOT NULL'
        );
    }

    public function test_alter_domain_set_default() : void
    {
        $builder = alter()->domain('email')
            ->setDefault("'default@example.com'");

        $this->assertAlterDomainQuery(
            $builder,
            "ALTER DOMAIN email SET DEFAULT 'default@example.com'"
        );
    }

    public function test_alter_domain_set_not_null() : void
    {
        $builder = alter()->domain('email')
            ->setNotNull();

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email SET NOT NULL'
        );
    }

    public function test_alter_domain_validate_constraint() : void
    {
        $builder = alter()->domain('email')
            ->validateConstraint('valid_email');

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email VALIDATE CONSTRAINT valid_email'
        );
    }

    public function test_create_domain_simple() : void
    {
        $builder = create()->domain('email')
            ->as(DataType::text());

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN email AS pg_catalog.text'
        );
    }

    public function test_create_domain_with_check() : void
    {
        $builder = create()->domain('positive_int')
            ->as(DataType::integer())
            ->check('VALUE > 0');

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN positive_int AS int CHECK (value > 0)'
        );
    }

    public function test_create_domain_with_collation() : void
    {
        $builder = create()->domain('email')
            ->as(DataType::text())
            ->collate('en_US');

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN email AS pg_catalog.text COLLATE "en_US"'
        );
    }

    public function test_create_domain_with_default() : void
    {
        $builder = create()->domain('email')
            ->as(DataType::text())
            ->default("'default@example.com'");

        $this->assertCreateDomainQuery(
            $builder,
            "CREATE DOMAIN email AS pg_catalog.text DEFAULT 'default@example.com'"
        );
    }

    public function test_create_domain_with_multiple_constraints() : void
    {
        $builder = create()->domain('email')
            ->as(DataType::text())
            ->notNull()
            ->check("VALUE ~ '^.+@.+\$'");

        $this->assertCreateDomainQuery(
            $builder,
            "CREATE DOMAIN email AS pg_catalog.text NOT NULL CHECK (value ~ '^.+@.+\$')"
        );
    }

    public function test_create_domain_with_named_constraint() : void
    {
        $builder = create()->domain('positive_int')
            ->as(DataType::integer())
            ->constraint('positive_check')
            ->check('VALUE > 0');

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN positive_int AS int CONSTRAINT positive_check CHECK (value > 0)'
        );
    }

    public function test_create_domain_with_not_null() : void
    {
        $builder = create()->domain('email')
            ->as(DataType::text())
            ->notNull();

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN email AS pg_catalog.text NOT NULL'
        );
    }

    public function test_create_domain_with_null() : void
    {
        $builder = create()->domain('email')
            ->as(DataType::text())
            ->null();

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN email AS pg_catalog.text NULL'
        );
    }

    public function test_create_domain_with_schema() : void
    {
        $builder = create()->domain('public.email')
            ->as(DataType::text());

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN public.email AS pg_catalog.text'
        );
    }

    public function test_drop_domain_cascade() : void
    {
        $builder = drop()->domain('email')
            ->cascade();

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN email CASCADE'
        );
    }

    public function test_drop_domain_if_exists() : void
    {
        $builder = drop()->domain('email')
            ->ifExists();

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN IF EXISTS email'
        );
    }

    public function test_drop_domain_if_exists_cascade() : void
    {
        $builder = drop()->domain('email')
            ->ifExists()
            ->cascade();

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN IF EXISTS email CASCADE'
        );
    }

    public function test_drop_domain_multiple() : void
    {
        $builder = drop()->domain('email', 'phone', 'url');

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN email, phone, url'
        );
    }

    public function test_drop_domain_restrict() : void
    {
        $builder = drop()->domain('email')
            ->restrict();

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN email'
        );
    }

    public function test_drop_domain_simple() : void
    {
        $builder = drop()->domain('email');

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN email'
        );
    }
}
