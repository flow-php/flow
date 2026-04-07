<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_boolean, literal, param, select, typed};
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class BooleanConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string, bool}>
     */
    public static function provide_boolean_strings() : \Generator
    {
        yield 'string true' => ['true', true];
        yield 'string false' => ['false', false];
        yield 'string t' => ['t', true];
        yield 'string f' => ['f', false];
        yield 'string yes' => ['yes', true];
        yield 'string no' => ['no', false];
        yield 'string 1' => ['1', true];
        yield 'string 0' => ['0', false];
    }

    /**
     * @return \Generator<string, array{bool, bool}>
     */
    public static function provide_boolean_values() : \Generator
    {
        yield 'true' => [true, true];
        yield 'false' => [false, false];
    }

    #[DataProvider('provide_boolean_values')]
    public function test_boolean_round_trip(bool $input, bool $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_boolean())->as('val'))->toSql(), [typed($input, ValueType::BOOL)]);

        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_boolean_strings')]
    public function test_boolean_string_conversion(string $input, bool $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_boolean())->as('val'))->toSql(), [$input]);

        self::assertSame($expected, $result);
    }

    public function test_null_boolean() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_boolean())->as('val'))->toSql());

        self::assertNull($result);
    }
}
