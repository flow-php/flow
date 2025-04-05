<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{lit, ref, row, str_entry, type_boolean, type_int, type_string};
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Tests\FlowTestCase;

final class ParameterTest extends FlowTestCase
{
    public function test_as_one_of() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertNull($parameter->as(row(str_entry('value', '42')), type_int(), type_boolean()));
        self::assertSame('42', $parameter->as(row(str_entry('value', '42')), type_string(), type_int()));
    }

    public function test_as_one_of_on_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('42')));

        self::assertSame('42', $parameter->as(row(), type_string(), type_int()));
        self::assertNull($parameter->as(row(), type_boolean()));
    }

    public function test_as_scalar() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertNull($parameter->as(row(str_entry('value', '42')), type_int()));
        self::assertSame('42', $parameter->as(row(str_entry('value', '42')), type_string()));
    }

    public function test_as_scalar_on_scalar_result() : void
    {
        $parameter = new Parameter(lit(ScalarResult::from('test')));

        self::assertNull($parameter->as(row(), type_int()));
        self::assertSame('test', $parameter->as(row(), type_string()));
    }
}
