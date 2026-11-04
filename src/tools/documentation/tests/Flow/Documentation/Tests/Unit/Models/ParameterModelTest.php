<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Models;

use Flow\Documentation\Models\ParameterModel;
use Flow\Documentation\Models\TypeModel;
use Flow\Documentation\Tests\Double\DefaultValues;
use PHPUnit\Framework\TestCase;
use Roave\BetterReflection\Reflection\ReflectionParameter;

final class ParameterModelTest extends TestCase
{
    public function test_a_default_value_is_exported_as_source_without_building_it(): void
    {
        $parameter = ParameterModel::fromReflection(ReflectionParameter::createFromClassNameAndMethod(
            DefaultValues::class,
            'refused',
            'value',
        ));

        static::assertTrue($parameter->hasDefaultValue);
        static::assertSame('new \Flow\Documentation\Tests\Double\RefusedDefault()', $parameter->defaultValue);
    }

    public function test_a_default_value_is_exported(): void
    {
        $parameter = ParameterModel::fromReflection(ReflectionParameter::createFromClassNameAndMethod(
            DefaultValues::class,
            'scalar',
            'value',
        ));

        static::assertTrue($parameter->hasDefaultValue);
        static::assertSame('5', $parameter->defaultValue);
    }

    public function test_a_nullable_type_is_exported_as_one_nullable_type(): void
    {
        $parameter = ParameterModel::fromReflection(ReflectionParameter::createFromClassNameAndMethod(
            DefaultValues::class,
            'nullable',
            'value',
        ));

        static::assertEquals([new TypeModel('int', null, true, false)], $parameter->type->types);
    }

    public function test_a_parameter_without_a_default_value_exports_none(): void
    {
        $parameter = ParameterModel::fromReflection(ReflectionParameter::createFromClassNameAndMethod(
            DefaultValues::class,
            'required',
            'value',
        ));

        static::assertFalse($parameter->hasDefaultValue);
        static::assertNull($parameter->defaultValue);
    }
}
