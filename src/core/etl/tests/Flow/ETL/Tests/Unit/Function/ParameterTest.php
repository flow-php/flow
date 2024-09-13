<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{object_entry, ref, row, str_entry, type_boolean, type_int, type_object, type_string};
use Flow\ETL\Function\Parameter;
use PHPUnit\Framework\TestCase;

final class ParameterTest extends TestCase
{
    public function test_as_object() : void
    {
        $parameter = new Parameter(ref('value'));

        $value = new \DateTimeImmutable();

        self::assertNull($parameter->as(row(object_entry('value', $value)), type_int()));
        self::assertSame($value, $parameter->as(row(object_entry('value', $value)), type_object(\DateTimeInterface::class)));
        self::assertSame($value, $parameter->as(row(object_entry('value', $value)), type_object(\DateTimeImmutable::class)));
        self::assertNull($parameter->as(row(object_entry('value', $value)), type_object(\DateTime::class)));
    }

    public function test_as_one_of() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertNull($parameter->as(row(str_entry('value', '42')), type_int(), type_boolean()));
        self::assertSame('42', $parameter->as(row(str_entry('value', '42')), type_string(), type_int()));
    }

    public function test_as_scalar() : void
    {
        $parameter = new Parameter(ref('value'));

        self::assertNull($parameter->as(row(str_entry('value', '42')), type_int()));
        self::assertSame('42', $parameter->as(row(str_entry('value', '42')), type_string()));
    }
}
