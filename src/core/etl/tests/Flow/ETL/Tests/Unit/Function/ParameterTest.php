<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, lit, ref, row, str_entry};
use function Flow\Types\DSL\{type_boolean, type_integer, type_string};
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Tests\FlowTestCase;

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
        $type = $parameter->asType(row());
        self::assertTrue($type->isValid('string_value'));
        self::assertFalse($type->isValid(42));

        $parameter = new Parameter(lit(42));
        $type = $parameter->asType(row());
        self::assertTrue($type->isValid(42));
        self::assertFalse($type->isValid('string'));

        $parameter = new Parameter(lit(true));
        $type = $parameter->asType(row());
        self::assertTrue($type->isValid(true));
        self::assertFalse($type->isValid(42));
    }

    public function test_as_type_with_reference() : void
    {
        $parameter = new Parameter(ref('value'));

        $type = $parameter->asType(row(str_entry('value', 'test')));
        self::assertTrue($type->isValid('test'));
        self::assertFalse($type->isValid(42));

        $type = $parameter->asType(row(int_entry('value', 42)));
        self::assertTrue($type->isValid(42));
        self::assertFalse($type->isValid('test'));
    }

    public function test_as_type_with_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('test')));
        $type = $parameter->asType(row());
        self::assertTrue($type->isValid('test'));
        self::assertFalse($type->isValid(42));

        $parameter = new Parameter(lit(ScalarResult::from(123)));
        $type = $parameter->asType(row());
        self::assertTrue($type->isValid(123));
        self::assertFalse($type->isValid('string'));
    }
}
