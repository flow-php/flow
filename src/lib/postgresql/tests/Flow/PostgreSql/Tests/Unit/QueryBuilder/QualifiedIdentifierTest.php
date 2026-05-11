<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder;

use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use PHPUnit\Framework\TestCase;

final class QualifiedIdentifierTest extends TestCase
{
    public function test_from_parts(): void
    {
        $identifier = QualifiedIdentifier::fromParts(['public', 'users']);

        static::assertSame(['public', 'users'], $identifier->parts());
        static::assertSame('users', $identifier->name());
        static::assertSame('public', $identifier->schema());
    }

    public function test_parse_both_quoted(): void
    {
        $identifier = QualifiedIdentifier::parse('"my.schema"."my.table"');

        static::assertSame(['my.schema', 'my.table'], $identifier->parts());
        static::assertSame('my.table', $identifier->name());
        static::assertSame('my.schema', $identifier->schema());
    }

    public function test_parse_empty_string(): void
    {
        $identifier = QualifiedIdentifier::parse('');

        static::assertSame([''], $identifier->parts());
        static::assertSame('', $identifier->name());
    }

    public function test_parse_identifier_with_underscores(): void
    {
        $identifier = QualifiedIdentifier::parse('my_schema.my_table');

        static::assertSame(['my_schema', 'my_table'], $identifier->parts());
    }

    public function test_parse_quoted_identifier_with_dots(): void
    {
        $identifier = QualifiedIdentifier::parse('"my.table.with.dots"');

        static::assertSame(['my.table.with.dots'], $identifier->parts());
        static::assertSame('my.table.with.dots', $identifier->name());
        static::assertNull($identifier->schema());
        static::assertFalse($identifier->hasSchema());
    }

    public function test_parse_quoted_schema_with_name(): void
    {
        $identifier = QualifiedIdentifier::parse('"my.schema".users');

        static::assertSame(['my.schema', 'users'], $identifier->parts());
        static::assertSame('users', $identifier->name());
        static::assertSame('my.schema', $identifier->schema());
    }

    public function test_parse_regular_identifier_not_quoted(): void
    {
        $identifier = QualifiedIdentifier::parse('simple_table');

        static::assertSame(['simple_table'], $identifier->parts());
    }

    public function test_parse_schema_qualified_name(): void
    {
        $identifier = QualifiedIdentifier::parse('public.users');

        static::assertSame(['public', 'users'], $identifier->parts());
        static::assertSame('users', $identifier->name());
        static::assertSame('public', $identifier->schema());
        static::assertTrue($identifier->hasSchema());
        static::assertSame(2, $identifier->count());
    }

    public function test_parse_schema_with_quoted_name(): void
    {
        $identifier = QualifiedIdentifier::parse('public."my.table"');

        static::assertSame(['public', 'my.table'], $identifier->parts());
        static::assertSame('my.table', $identifier->name());
        static::assertSame('public', $identifier->schema());
        static::assertTrue($identifier->hasSchema());
    }

    public function test_parse_simple_name(): void
    {
        $identifier = QualifiedIdentifier::parse('users');

        static::assertSame(['users'], $identifier->parts());
        static::assertSame('users', $identifier->name());
        static::assertNull($identifier->schema());
        static::assertFalse($identifier->hasSchema());
        static::assertSame(1, $identifier->count());
    }

    public function test_parse_three_part_identifier(): void
    {
        $identifier = QualifiedIdentifier::parse('myschema.mytable.mycolumn');

        static::assertSame(['myschema', 'mytable', 'mycolumn'], $identifier->parts());
        static::assertSame('mycolumn', $identifier->name());
        static::assertSame('myschema', $identifier->schema());
        static::assertSame('mytable', $identifier->table());
        static::assertSame('mycolumn', $identifier->column());
        static::assertSame(3, $identifier->count());
    }

    public function test_table_for_single_part_identifier(): void
    {
        $identifier = QualifiedIdentifier::parse('email');

        static::assertNull($identifier->table());
        static::assertSame('email', $identifier->column());
    }

    public function test_table_for_two_part_identifier(): void
    {
        $identifier = QualifiedIdentifier::parse('users.email');

        static::assertSame('users', $identifier->table());
        static::assertSame('email', $identifier->column());
    }
}
