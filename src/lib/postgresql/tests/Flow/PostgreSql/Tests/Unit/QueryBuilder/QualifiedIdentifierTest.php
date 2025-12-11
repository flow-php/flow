<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder;

use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use PHPUnit\Framework\TestCase;

final class QualifiedIdentifierTest extends TestCase
{
    public function test_from_parts() : void
    {
        $identifier = QualifiedIdentifier::fromParts(['public', 'users']);

        self::assertSame(['public', 'users'], $identifier->parts());
        self::assertSame('users', $identifier->name());
        self::assertSame('public', $identifier->schema());
    }

    public function test_parse_both_quoted() : void
    {
        $identifier = QualifiedIdentifier::parse('"my.schema"."my.table"');

        self::assertSame(['my.schema', 'my.table'], $identifier->parts());
        self::assertSame('my.table', $identifier->name());
        self::assertSame('my.schema', $identifier->schema());
    }

    public function test_parse_empty_string() : void
    {
        $identifier = QualifiedIdentifier::parse('');

        self::assertSame([''], $identifier->parts());
        self::assertSame('', $identifier->name());
    }

    public function test_parse_identifier_with_underscores() : void
    {
        $identifier = QualifiedIdentifier::parse('my_schema.my_table');

        self::assertSame(['my_schema', 'my_table'], $identifier->parts());
    }

    public function test_parse_quoted_identifier_with_dots() : void
    {
        $identifier = QualifiedIdentifier::parse('"my.table.with.dots"');

        self::assertSame(['my.table.with.dots'], $identifier->parts());
        self::assertSame('my.table.with.dots', $identifier->name());
        self::assertNull($identifier->schema());
        self::assertFalse($identifier->hasSchema());
    }

    public function test_parse_quoted_schema_with_name() : void
    {
        $identifier = QualifiedIdentifier::parse('"my.schema".users');

        self::assertSame(['my.schema', 'users'], $identifier->parts());
        self::assertSame('users', $identifier->name());
        self::assertSame('my.schema', $identifier->schema());
    }

    public function test_parse_regular_identifier_not_quoted() : void
    {
        $identifier = QualifiedIdentifier::parse('simple_table');

        self::assertSame(['simple_table'], $identifier->parts());
    }

    public function test_parse_schema_qualified_name() : void
    {
        $identifier = QualifiedIdentifier::parse('public.users');

        self::assertSame(['public', 'users'], $identifier->parts());
        self::assertSame('users', $identifier->name());
        self::assertSame('public', $identifier->schema());
        self::assertTrue($identifier->hasSchema());
        self::assertSame(2, $identifier->count());
    }

    public function test_parse_schema_with_quoted_name() : void
    {
        $identifier = QualifiedIdentifier::parse('public."my.table"');

        self::assertSame(['public', 'my.table'], $identifier->parts());
        self::assertSame('my.table', $identifier->name());
        self::assertSame('public', $identifier->schema());
        self::assertTrue($identifier->hasSchema());
    }

    public function test_parse_simple_name() : void
    {
        $identifier = QualifiedIdentifier::parse('users');

        self::assertSame(['users'], $identifier->parts());
        self::assertSame('users', $identifier->name());
        self::assertNull($identifier->schema());
        self::assertFalse($identifier->hasSchema());
        self::assertSame(1, $identifier->count());
    }

    public function test_parse_three_part_identifier() : void
    {
        $identifier = QualifiedIdentifier::parse('myschema.mytable.mycolumn');

        self::assertSame(['myschema', 'mytable', 'mycolumn'], $identifier->parts());
        self::assertSame('mycolumn', $identifier->name());
        self::assertSame('myschema', $identifier->schema());
        self::assertSame('mytable', $identifier->table());
        self::assertSame('mycolumn', $identifier->column());
        self::assertSame(3, $identifier->count());
    }

    public function test_table_for_single_part_identifier() : void
    {
        $identifier = QualifiedIdentifier::parse('email');

        self::assertNull($identifier->table());
        self::assertSame('email', $identifier->column());
    }

    public function test_table_for_two_part_identifier() : void
    {
        $identifier = QualifiedIdentifier::parse('users.email');

        self::assertSame('users', $identifier->table());
        self::assertSame('email', $identifier->column());
    }
}
