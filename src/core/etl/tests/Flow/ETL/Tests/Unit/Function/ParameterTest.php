<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, lit, ref, row, str_entry};
use function Flow\Types\DSL\{type_boolean, type_integer, type_string};
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\{BooleanType, IntegerType, StringType};

final class ParameterTest extends FlowTestCase
{
    public function test_as_one_of() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertNull($parameter->as(row(str_entry('value', '42')), type_integer(), type_boolean()));
        self::assertSame('42', $parameter->as(row(str_entry('value', '42')), type_string(), type_integer()));
    }

    public function test_as_one_of_on_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('42')));

        self::assertSame('42', $parameter->as(row(), type_string(), type_integer()));
        self::assertNull($parameter->as(row(), type_boolean()));
    }

    public function test_as_scalar() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertNull($parameter->as(row(str_entry('value', '42')), type_integer()));
        self::assertSame('42', $parameter->as(row(str_entry('value', '42')), type_string()));
    }

    public function test_as_scalar_on_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('test')));

        self::assertNull($parameter->as(row(), type_integer()));
        self::assertSame('test', $parameter->as(row(), type_string()));
    }

    public function test_as_type_with_literal_value() : void
    {
        $parameter = new Parameter(lit('string_value'));
        self::assertInstanceOf(StringType::class, $parameter->asType(row()));

        $parameter = new Parameter(lit(42));
        self::assertInstanceOf(IntegerType::class, $parameter->asType(row()));

        $parameter = new Parameter(lit(true));
        self::assertInstanceOf(BooleanType::class, $parameter->asType(row()));
    }

    public function test_as_type_with_reference() : void
    {
        $parameter = new Parameter(ref('value'));
        self::assertInstanceOf(StringType::class, $parameter->asType(row(str_entry('value', 'test'))));
        self::assertInstanceOf(IntegerType::class, $parameter->asType(row(int_entry('value', 42))));
    }

    public function test_as_type_with_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('test')));
        self::assertInstanceOf(StringType::class, $parameter->asType(row()));

        $parameter = new Parameter(lit(ScalarResult::from(123)));
        self::assertInstanceOf(IntegerType::class, $parameter->asType(row()));
    }
}
