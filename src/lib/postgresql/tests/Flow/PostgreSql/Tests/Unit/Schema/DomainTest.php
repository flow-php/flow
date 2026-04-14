<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\{column_type_integer, column_type_varchar, schema_check, schema_domain};

use PHPUnit\Framework\TestCase;

final class DomainTest extends TestCase
{
    public function test_domain_construction() : void
    {
        $domain = schema_domain('email_address', column_type_varchar(255));

        self::assertSame('email_address', $domain->name);
        self::assertTrue($domain->nullable);
        self::assertNull($domain->default);
        self::assertSame([], $domain->checkConstraints);
    }

    public function test_domain_with_constraints() : void
    {
        $domain = schema_domain(
            'positive_int',
            column_type_integer(),
            nullable: false,
            default: 0,
            checkConstraints: [schema_check('VALUE > 0', 'chk_positive')],
        );

        self::assertFalse($domain->nullable);
        self::assertSame('0', $domain->default);
        self::assertCount(1, $domain->checkConstraints);
        self::assertSame('VALUE > 0', $domain->checkConstraints[0]->expression);
    }

    public function test_to_sql_generates_create_domain() : void
    {
        self::assertSame(
            'CREATE DOMAIN email_address AS varchar(255)',
            schema_domain('email_address', column_type_varchar(255))->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_create_domain_with_constraints() : void
    {
        self::assertSame(
            'CREATE DOMAIN positive_int AS int NOT NULL DEFAULT 0 CONSTRAINT chk_positive CHECK (value > 0)',
            schema_domain(
                'positive_int',
                column_type_integer(),
                nullable: false,
                default: 0,
                checkConstraints: [schema_check('VALUE > 0', 'chk_positive')],
            )->toSql()->toSql(),
        );
    }
}
