<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter_domain, create_domain, drop_domain};

final class DomainBuilderTest extends PGQueryTestCase
{
    public function test_alter_domain_add_constraint() : void
    {
        $builder = alter_domain('email')
            ->addConstraint('valid_email', "VALUE ~ '^.+@.+$'");

        $this->assertAlterDomainQuery(
            $builder,
            "ALTER DOMAIN email ADD CONSTRAINT valid_email CHECK (value ~ '^.+@.+$')"
        );
    }

    public function test_alter_domain_drop_constraint() : void
    {
        $builder = alter_domain('email')
            ->dropConstraint('valid_email');

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email DROP CONSTRAINT valid_email'
        );
    }

    public function test_alter_domain_drop_constraint_cascade() : void
    {
        $builder = alter_domain('email')
            ->dropConstraint('valid_email')
            ->cascade();

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email DROP CONSTRAINT valid_email CASCADE'
        );
    }

    public function test_alter_domain_drop_default() : void
    {
        $builder = alter_domain('email')
            ->dropDefault();

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email DROP DEFAULT'
        );
    }

    public function test_alter_domain_drop_not_null() : void
    {
        $builder = alter_domain('email')
            ->dropNotNull();

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email DROP NOT NULL'
        );
    }

    public function test_alter_domain_set_default() : void
    {
        $builder = alter_domain('email')
            ->setDefault("'default@example.com'");

        $this->assertAlterDomainQuery(
            $builder,
            "ALTER DOMAIN email SET DEFAULT 'default@example.com'"
        );
    }

    public function test_alter_domain_set_not_null() : void
    {
        $builder = alter_domain('email')
            ->setNotNull();

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email SET NOT NULL'
        );
    }

    public function test_alter_domain_validate_constraint() : void
    {
        $builder = alter_domain('email')
            ->validateConstraint('valid_email');

        $this->assertAlterDomainQuery(
            $builder,
            'ALTER DOMAIN email VALIDATE CONSTRAINT valid_email'
        );
    }

    public function test_create_domain_simple() : void
    {
        $builder = create_domain('email')
            ->as('text');

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN email AS text'
        );
    }

    public function test_create_domain_with_check() : void
    {
        $builder = create_domain('positive_int')
            ->as('int4')
            ->check('VALUE > 0');

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN positive_int AS int4 CHECK (value > 0)'
        );
    }

    public function test_create_domain_with_collation() : void
    {
        $builder = create_domain('email')
            ->as('text')
            ->collate('en_US');

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN email AS text COLLATE "en_US"'
        );
    }

    public function test_create_domain_with_default() : void
    {
        $builder = create_domain('email')
            ->as('text')
            ->default("'default@example.com'");

        $this->assertCreateDomainQuery(
            $builder,
            "CREATE DOMAIN email AS text DEFAULT 'default@example.com'"
        );
    }

    public function test_create_domain_with_multiple_constraints() : void
    {
        $builder = create_domain('email')
            ->as('text')
            ->notNull()
            ->check("VALUE ~ '^.+@.+\$'");

        $this->assertCreateDomainQuery(
            $builder,
            "CREATE DOMAIN email AS text NOT NULL CHECK (value ~ '^.+@.+\$')"
        );
    }

    public function test_create_domain_with_named_constraint() : void
    {
        $builder = create_domain('positive_int')
            ->as('int4')
            ->constraint('positive_check')
            ->check('VALUE > 0');

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN positive_int AS int4 CONSTRAINT positive_check CHECK (value > 0)'
        );
    }

    public function test_create_domain_with_not_null() : void
    {
        $builder = create_domain('email')
            ->as('text')
            ->notNull();

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN email AS text NOT NULL'
        );
    }

    public function test_create_domain_with_null() : void
    {
        $builder = create_domain('email')
            ->as('text')
            ->null();

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN email AS text NULL'
        );
    }

    public function test_create_domain_with_schema() : void
    {
        $builder = create_domain('public.email')
            ->as('text');

        $this->assertCreateDomainQuery(
            $builder,
            'CREATE DOMAIN public.email AS text'
        );
    }

    public function test_drop_domain_cascade() : void
    {
        $builder = drop_domain('email')
            ->cascade();

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN email CASCADE'
        );
    }

    public function test_drop_domain_if_exists() : void
    {
        $builder = drop_domain('email')
            ->ifExists();

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN IF EXISTS email'
        );
    }

    public function test_drop_domain_if_exists_cascade() : void
    {
        $builder = drop_domain('email')
            ->ifExists()
            ->cascade();

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN IF EXISTS email CASCADE'
        );
    }

    public function test_drop_domain_multiple() : void
    {
        $builder = drop_domain('email', 'phone', 'url');

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN email, phone, url'
        );
    }

    public function test_drop_domain_restrict() : void
    {
        $builder = drop_domain('email')
            ->restrict();

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN email'
        );
    }

    public function test_drop_domain_simple() : void
    {
        $builder = drop_domain('email');

        $this->assertDropDomainQuery(
            $builder,
            'DROP DOMAIN email'
        );
    }
}
