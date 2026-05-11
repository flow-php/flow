<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\schema_check;
use function Flow\PostgreSql\DSL\schema_domain;

final class DomainTest extends TestCase
{
    public function test_domain_construction(): void
    {
        $domain = schema_domain('email_address', column_type_varchar(255));

        static::assertSame('email_address', $domain->name);
        static::assertTrue($domain->nullable);
        static::assertNull($domain->default);
        static::assertSame([], $domain->checkConstraints);
    }

    public function test_domain_with_constraints(): void
    {
        $domain = schema_domain(
            'positive_int',
            column_type_integer(),
            nullable: false,
            default: 0,
            checkConstraints: [schema_check('VALUE > 0', 'chk_positive')],
        );

        static::assertFalse($domain->nullable);
        static::assertSame('0', $domain->default);
        static::assertCount(1, $domain->checkConstraints);
        static::assertSame('value > 0', $domain->checkConstraints[0]->expression);
    }

    public function test_to_sql_generates_create_domain(): void
    {
        static::assertSame(
            'CREATE DOMAIN email_address AS varchar(255)',
            schema_domain('email_address', column_type_varchar(255))->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_create_domain_with_constraints(): void
    {
        static::assertSame(
            'CREATE DOMAIN positive_int AS int NOT NULL DEFAULT 0 CONSTRAINT chk_positive CHECK (value > 0)',
            schema_domain(
                'positive_int',
                column_type_integer(),
                nullable: false,
                default: 0,
                checkConstraints: [schema_check('VALUE > 0', 'chk_positive')],
            )
                ->toSql()
                ->toSql(),
        );
    }
}
