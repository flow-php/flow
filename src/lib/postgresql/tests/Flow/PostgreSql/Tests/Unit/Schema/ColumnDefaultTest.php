<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\ColumnDefault;
use Flow\PostgreSql\Schema\DefaultKind;
use PHPUnit\Framework\TestCase;

final class ColumnDefaultTest extends TestCase
{
    public function test_bare_constant_uses_column_type_as_effective_type(): void
    {
        $default = ColumnDefault::fromExpression('5', ColumnType::integer());

        static::assertSame(DefaultKind::CONSTANT, $default->kind);
        static::assertSame('5', $default->literal);
        static::assertNotNull($default->effectiveType);
        static::assertTrue($default->effectiveType->isSameBaseType(ColumnType::integer()));
    }

    public function test_bool_constants_render_as_keywords(): void
    {
        static::assertSame('true', ColumnDefault::fromExpression('true', ColumnType::boolean())->literal);
        static::assertSame('false', ColumnDefault::fromExpression('false', ColumnType::boolean())->literal);
    }

    public function test_constant_and_expression_are_never_equal(): void
    {
        $constant = ColumnDefault::fromExpression("'now()'", ColumnType::text());
        $expression = ColumnDefault::fromExpression('now()', ColumnType::timestamptz());

        static::assertSame(DefaultKind::CONSTANT, $constant->kind);
        static::assertSame(DefaultKind::EXPRESSION, $expression->kind);
        static::assertFalse($constant->equals($expression));
    }

    public function test_equal_expression_defaults_compare_equal(): void
    {
        static::assertTrue(ColumnDefault::fromExpression(
            'now()',
            ColumnType::timestamptz(),
        )->equals(ColumnDefault::fromExpression('now()', ColumnType::timestamptz())));
    }

    public function test_expression_default_strips_implicit_casts_like_generation_expressions(): void
    {
        $default = ColumnDefault::fromExpression('upper(name::text)', ColumnType::text());

        static::assertSame(DefaultKind::EXPRESSION, $default->kind);
        static::assertSame('upper(name)', $default->literal);
        static::assertTrue($default->equals(ColumnDefault::fromExpression('upper(name)', ColumnType::text())));
    }

    public function test_float_constant_is_rendered_verbatim(): void
    {
        static::assertSame('3.14', ColumnDefault::fromExpression('3.14', ColumnType::doublePrecision())->literal);
    }

    public function test_implicit_varchar_cast_equals_bare_string(): void
    {
        static::assertTrue(ColumnDefault::fromExpression(
            "'pending'::character varying",
            ColumnType::varchar(255),
        )->equals(ColumnDefault::fromExpression("'pending'", ColumnType::varchar(255))));
    }

    public function test_nullable_equals(): void
    {
        $default = ColumnDefault::fromExpression('0', ColumnType::integer());

        static::assertTrue(ColumnDefault::nullableEquals(null, null));
        static::assertFalse(ColumnDefault::nullableEquals($default, null));
        static::assertFalse(ColumnDefault::nullableEquals(null, $default));
        static::assertTrue(ColumnDefault::nullableEquals($default, $default));
    }

    public function test_round_trips_through_normalize_and_from_array(): void
    {
        foreach ([
            ColumnDefault::fromExpression("'pending'", ColumnType::varchar(255)),
            ColumnDefault::fromExpression("'0'::double precision", ColumnType::numeric(10, 3)),
            ColumnDefault::fromExpression('now()', ColumnType::timestamptz()),
            ColumnDefault::fromExpression('5', ColumnType::integer()),
        ] as $default) {
            static::assertTrue($default->equals(ColumnDefault::fromArray($default->normalize())));
        }
    }

    public function test_same_base_type_cast_equals_bare_constant(): void
    {
        static::assertTrue(ColumnDefault::fromExpression("'0'::numeric", ColumnType::numeric(
            10,
            3,
        ))->equals(ColumnDefault::fromExpression("'0'", ColumnType::numeric(10, 3))));
    }

    public function test_stale_cast_is_not_equal_to_clean_default(): void
    {
        $stale = ColumnDefault::fromExpression("'0'::double precision", ColumnType::numeric(10, 3));
        $clean = ColumnDefault::fromExpression("'0'", ColumnType::numeric(10, 3));

        static::assertSame("'0'", $stale->literal);
        static::assertSame("'0'", $clean->literal);
        static::assertFalse($stale->equals($clean));
    }

    public function test_string_constant_is_quoted_and_escaped(): void
    {
        static::assertSame("'it''s'", ColumnDefault::fromExpression("'it''s'", ColumnType::text())->literal);
    }
}
