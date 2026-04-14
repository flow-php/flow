<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\{column_type_integer, column_type_text, column_type_varchar, schema_column};

use Flow\PostgreSql\Schema\IdentityGeneration;
use PHPUnit\Framework\TestCase;

final class ColumnTest extends TestCase
{
    public function test_column_construction() : void
    {
        $column = schema_column('id', column_type_integer(), nullable: false);

        self::assertSame('id', $column->name);
        self::assertFalse($column->nullable);
        self::assertNull($column->default);
    }

    public function test_column_generated() : void
    {
        $column = schema_column('full_name', column_type_text(), isGenerated: true, generationExpression: "first_name || ' ' || last_name");

        self::assertTrue($column->isGenerated);
        self::assertSame("(first_name || ' ') || last_name", $column->generationExpression);
    }

    public function test_column_identity() : void
    {
        $column = schema_column('id', column_type_integer(), nullable: false, isIdentity: true, identityGeneration: IdentityGeneration::ALWAYS);

        self::assertTrue($column->isIdentity);
        self::assertSame(IdentityGeneration::ALWAYS, $column->identityGeneration);
    }

    public function test_column_with_default() : void
    {
        $column = schema_column('name', column_type_varchar(255), default: 'unknown');

        self::assertSame("'unknown'", $column->default);
    }

    public function test_is_equal_for_identical_columns() : void
    {
        $a = schema_column('id', column_type_integer(), nullable: false);
        $b = schema_column('id', column_type_integer(), nullable: false);

        self::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_default_differs() : void
    {
        $a = schema_column('name', column_type_text(), default: 'hello');
        $b = schema_column('name', column_type_text(), default: 'world');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_generation_expression_differs() : void
    {
        $a = schema_column('full_name', column_type_text(), isGenerated: true, generationExpression: 'first || last');
        $b = schema_column('full_name', column_type_text(), isGenerated: true, generationExpression: "first || ' ' || last");

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_identity_differs() : void
    {
        $a = schema_column('id', column_type_integer(), nullable: false, isIdentity: true, identityGeneration: IdentityGeneration::ALWAYS);
        $b = schema_column('id', column_type_integer(), nullable: false, isIdentity: false);

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs() : void
    {
        $a = schema_column('id', column_type_integer(), nullable: false);
        $b = schema_column('user_id', column_type_integer(), nullable: false);

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_nullable_differs() : void
    {
        $a = schema_column('name', column_type_text(), nullable: false);
        $b = schema_column('name', column_type_text(), nullable: true);

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_type_differs() : void
    {
        $a = schema_column('name', column_type_text(), nullable: false);
        $b = schema_column('name', column_type_varchar(255), nullable: false);

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_ignores_ordinal_position() : void
    {
        $a = schema_column('id', column_type_integer(), nullable: false, ordinalPosition: 1);
        $b = schema_column('id', column_type_integer(), nullable: false, ordinalPosition: 5);

        self::assertTrue($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_default_differs() : void
    {
        $a = schema_column('name', column_type_text(), default: 'a');
        $b = schema_column('name', column_type_text(), default: 'b');

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_nullable_differs() : void
    {
        $a = schema_column('name', column_type_text(), nullable: false);
        $b = schema_column('name', column_type_text(), nullable: true);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_type_differs() : void
    {
        $a = schema_column('name', column_type_text(), nullable: false);
        $b = schema_column('name', column_type_varchar(255), nullable: false);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs() : void
    {
        $a = schema_column('id', column_type_integer(), nullable: false);
        $b = schema_column('user_id', column_type_integer(), nullable: false);

        self::assertTrue($a->isEqualStructure($b));
    }
}
