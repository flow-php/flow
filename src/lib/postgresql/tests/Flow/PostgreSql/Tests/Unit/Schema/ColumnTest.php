<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\IdentityGeneration;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\schema_column;

final class ColumnTest extends TestCase
{
    public function test_column_construction(): void
    {
        $column = schema_column('id', column_type_integer(), nullable: false);

        static::assertSame('id', $column->name);
        static::assertFalse($column->nullable);
        static::assertNull($column->default);
    }

    public function test_column_generated(): void
    {
        $column = schema_column(
            'full_name',
            column_type_text(),
            isGenerated: true,
            generationExpression: "first_name || ' ' || last_name",
        );

        static::assertTrue($column->isGenerated);
        static::assertSame("(first_name || ' ') || last_name", $column->generationExpression);
    }

    public function test_column_identity(): void
    {
        $column = schema_column(
            'id',
            column_type_integer(),
            nullable: false,
            isIdentity: true,
            identityGeneration: IdentityGeneration::ALWAYS,
        );

        static::assertTrue($column->isIdentity);
        static::assertSame(IdentityGeneration::ALWAYS, $column->identityGeneration);
    }

    public function test_column_with_default(): void
    {
        $column = schema_column('name', column_type_varchar(255), default: 'unknown');

        static::assertSame("'unknown'", $column->default);
    }

    public function test_is_equal_for_identical_columns(): void
    {
        $a = schema_column('id', column_type_integer(), nullable: false);
        $b = schema_column('id', column_type_integer(), nullable: false);

        static::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_default_differs(): void
    {
        $a = schema_column('name', column_type_text(), default: 'hello');
        $b = schema_column('name', column_type_text(), default: 'world');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_generation_expression_differs(): void
    {
        $a = schema_column('full_name', column_type_text(), isGenerated: true, generationExpression: 'first || last');
        $b = schema_column(
            'full_name',
            column_type_text(),
            isGenerated: true,
            generationExpression: "first || ' ' || last",
        );

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_identity_differs(): void
    {
        $a = schema_column(
            'id',
            column_type_integer(),
            nullable: false,
            isIdentity: true,
            identityGeneration: IdentityGeneration::ALWAYS,
        );
        $b = schema_column('id', column_type_integer(), nullable: false, isIdentity: false);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs(): void
    {
        $a = schema_column('id', column_type_integer(), nullable: false);
        $b = schema_column('user_id', column_type_integer(), nullable: false);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_nullable_differs(): void
    {
        $a = schema_column('name', column_type_text(), nullable: false);
        $b = schema_column('name', column_type_text(), nullable: true);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_type_differs(): void
    {
        $a = schema_column('name', column_type_text(), nullable: false);
        $b = schema_column('name', column_type_varchar(255), nullable: false);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_ignores_ordinal_position(): void
    {
        $a = schema_column('id', column_type_integer(), nullable: false, ordinalPosition: 1);
        $b = schema_column('id', column_type_integer(), nullable: false, ordinalPosition: 5);

        static::assertTrue($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_default_differs(): void
    {
        $a = schema_column('name', column_type_text(), default: 'a');
        $b = schema_column('name', column_type_text(), default: 'b');

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_nullable_differs(): void
    {
        $a = schema_column('name', column_type_text(), nullable: false);
        $b = schema_column('name', column_type_text(), nullable: true);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_type_differs(): void
    {
        $a = schema_column('name', column_type_text(), nullable: false);
        $b = schema_column('name', column_type_varchar(255), nullable: false);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs(): void
    {
        $a = schema_column('id', column_type_integer(), nullable: false);
        $b = schema_column('user_id', column_type_integer(), nullable: false);

        static::assertTrue($a->isEqualStructure($b));
    }
}
