<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Models;

use Flow\Documentation\Models\ParameterModel;
use Flow\Documentation\Tests\Double\DefaultValues;
use PHPUnit\Framework\TestCase;
use ReflectionParameter;
use RuntimeException;

final class ParameterModelTest extends TestCase
{
    public function test_a_default_value_that_cannot_be_built_propagates(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('The extension refused to load');

        ParameterModel::fromReflection(new ReflectionParameter([DefaultValues::class, 'refused'], 'value'));
    }

    public function test_a_default_value_is_exported(): void
    {
        $parameter = ParameterModel::fromReflection(new ReflectionParameter([DefaultValues::class, 'scalar'], 'value'));

        static::assertTrue($parameter->hasDefaultValue);
        static::assertSame('5', $parameter->defaultValue);
    }

    public function test_a_parameter_without_a_default_value_exports_none(): void
    {
        $parameter = ParameterModel::fromReflection(
            new ReflectionParameter([DefaultValues::class, 'required'], 'value'),
        );

        static::assertFalse($parameter->hasDefaultValue);
        static::assertNull($parameter->defaultValue);
    }
}
