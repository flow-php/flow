<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Unit\Options;

use Flow\CLI\Options\TypedOption;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\{ArrayInput, InputDefinition, InputOption};

final class TypedOptionTest extends TestCase
{
    public function test_getting_bool_nullable_value() : void
    {
        $option = new InputOption('option', null, InputOption::VALUE_OPTIONAL);
        $definition = new InputDefinition([$option]);

        self::assertTrue((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => 'true'], $definition)));
        self::assertTrue((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => 'on'], $definition)));
        self::assertTrue((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => '1'], $definition)));
        self::assertTrue((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => 1], $definition)));

        self::assertNull((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => 'lol'], $definition)));
        self::assertNull((new TypedOption('option'))->asBoolNullable(new ArrayInput([], $definition)));
    }

    public function test_getting_bool_nullable_value_with_default_definition() : void
    {
        $option = new InputOption('option', null, InputOption::VALUE_OPTIONAL, '', false);
        $definition = new InputDefinition([$option]);

        self::assertTrue((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => 'true'], $definition)));
        self::assertTrue((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => 'on'], $definition)));
        self::assertTrue((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => '1'], $definition)));
        self::assertTrue((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => 1], $definition)));

        self::assertNull((new TypedOption('option'))->asBoolNullable(new ArrayInput(['--option' => 'lol'], $definition)));
        self::assertFalse((new TypedOption('option'))->asBoolNullable(new ArrayInput([], $definition)));
    }

    public function test_getting_bool_value() : void
    {
        $option = new InputOption('option', null, InputOption::VALUE_NONE);
        $definition = new InputDefinition([$option]);

        self::assertTrue((new TypedOption('option'))->asBool(new ArrayInput(['--option' => null], $definition)));
        self::assertFalse((new TypedOption('option'))->asBool(new ArrayInput([], $definition)));
    }

    public function test_getting_int_value() : void
    {
        $option = new InputOption('option', null, InputOption::VALUE_REQUIRED);
        $definition = new InputDefinition([$option]);

        self::assertSame(1, (new TypedOption('option'))->asInt(new ArrayInput(['--option' => '1'], $definition)));
        self::assertSame(1, (new TypedOption('option'))->asInt(new ArrayInput(['--option' => 1], $definition)));
        self::assertSame(10, (new TypedOption('option'))->asInt(new ArrayInput([], $definition), 10));

        $this->expectException(InvalidArgumentException::class);
        self::assertNull((new TypedOption('option'))->asInt(new ArrayInput([], $definition)));
    }

    public function test_getting_list_of_strings() : void
    {
        $option = new InputOption('option', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY);
        $definition = new InputDefinition([$option]);

        self::assertEquals(['value1', 'value2', '1'], (new TypedOption('option'))->asListOfStrings(new ArrayInput(['--option' => ['value1', 'value2', 1]], $definition)));
        self::assertEquals([], (new TypedOption('option'))->asListOfStrings(new ArrayInput([], $definition)));
    }

    public function test_getting_list_of_strings_nullable() : void
    {
        $option = new InputOption('option', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY);
        $definition = new InputDefinition([$option]);

        self::assertEquals(['value1', 'value2'], (new TypedOption('option'))->asListOfStringsNullable(new ArrayInput(['--option' => ['value1', 'value2']], $definition)));
        self::assertNull((new TypedOption('option'))->asListOfStringsNullable(new ArrayInput([], $definition)));
    }

    public function test_getting_string_value() : void
    {
        $option = new InputOption('option', null, InputOption::VALUE_REQUIRED);
        $definition = new InputDefinition([$option]);

        self::assertSame('value', (new TypedOption('option'))->asString(new ArrayInput(['--option' => 'value'], $definition)));
        self::assertSame('default value', (new TypedOption('option'))->asString(new ArrayInput([], $definition), 'default value'));

        $this->expectException(InvalidArgumentException::class);
        self::assertNull((new TypedOption('option'))->asString(new ArrayInput([], $definition)));
    }
}
