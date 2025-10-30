<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use Flow\Filesystem\Path\Options;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\OptionsResolver\Exception\MissingOptionsException;

final class OptionsTest extends TestCase
{
    public static function provider_case_sensitivity_scenarios() : \Generator
    {
        yield 'lowercase key' => ['content-type', 'text/csv', 'content-type'];
        yield 'uppercase key' => ['CONTENT-TYPE', 'text/csv', 'content-type'];
        yield 'mixed case key' => ['Content-Type', 'text/csv', 'content-type'];
        yield 'camelCase key' => ['ContentType', 'application/json', 'contenttype'];
    }

    public static function provider_constructor_with_various_input_types() : \Generator
    {
        yield 'string keys and values' => [
            ['content-type' => 'text/csv', 'charset' => 'utf-8'],
            ['content-type' => 'text/csv', 'charset' => 'utf-8'],
        ];

        yield 'numeric keys converted to strings' => [
            [0 => 'value1', 1 => 'value2'],
            ['0' => 'value1', '1' => 'value2'],
        ];

        yield 'numeric values kept as integers' => [
            ['timeout' => 30, 'retries' => 3],
            ['timeout' => 30, 'retries' => 3],
        ];

        yield 'boolean values kept as booleans' => [
            ['debug' => true, 'cache' => false],
            ['debug' => true, 'cache' => false],
        ];

        yield 'mixed case keys normalized to lowercase' => [
            ['Content-Type' => 'text/csv', 'CHARSET' => 'utf-8', 'Timeout' => '30'],
            ['content-type' => 'text/csv', 'charset' => 'utf-8', 'timeout' => '30'],
        ];
    }

    public static function provider_get_as_string_scenarios() : \Generator
    {
        yield 'existing option with no default' => ['content-type', null, 'text/csv'];
        yield 'existing option with default' => ['content-type', 'fallback', 'text/csv'];
        yield 'non-existing option with default' => ['missing', 'default-value', 'default-value'];
        yield 'non-existing option without default' => ['missing', null, null];
    }

    public static function provider_set_when_empty_scenarios() : \Generator
    {
        yield 'set when option does not exist' => [
            ['initial' => 'value'],
            'new-option',
            'new-value',
            ['initial' => 'value', 'new-option' => 'new-value'],
        ];

        yield 'do not set when option exists' => [
            ['content-type' => 'text/csv'],
            'content-type',
            'text/json',
            ['content-type' => 'text/csv'],
        ];

        yield 'set when checking with different case' => [
            ['content-type' => 'text/csv'],
            'Content-Type',
            'text/json',
            ['content-type' => 'text/csv'],
        ];
    }

    public function test_assert_has_throws_exception_for_missing_option() : void
    {
        $options = new Options(['content-type' => 'text/csv']);

        $this->expectException(MissingOptionsException::class);
        $this->expectExceptionMessage("Option 'missing-option' is missing in Path object.");

        $options->assertHas('missing-option');
    }

    public function test_assert_has_with_existing_option() : void
    {
        $options = new Options(['content-type' => 'text/csv']);

        $options->assertHas('content-type');

        $this->expectNotToPerformAssertions();
    }

    public function test_bug_values_not_cast_to_string_in_constructor() : void
    {
        $options = new Options([
            'timeout' => 30,
            'retries' => 3,
            'debug' => true,
            'cache' => false,
        ]);

        $array = $options->toArray();

        self::assertIsInt($array['timeout']);
        self::assertIsInt($array['retries']);
        self::assertIsBool($array['debug']);
        self::assertIsBool($array['cache']);

        self::assertSame('30', $options->getAsString('timeout'));
        self::assertSame('3', $options->getAsString('retries'));
        self::assertSame('true', $options->getAsString('debug'));
        self::assertSame('false', $options->getAsString('cache'));
    }

    #[DataProvider('provider_case_sensitivity_scenarios')]
    public function test_case_insensitive_has_check(string $inputKey, string $value, string $normalizedKey) : void
    {
        $options = new Options([$inputKey => $value]);

        self::assertTrue($options->has($inputKey));
        self::assertTrue($options->has(\strtoupper($inputKey)));
        self::assertTrue($options->has(\strtolower($inputKey)));
        self::assertTrue($options->has($normalizedKey));
    }

    public function test_constructor_with_empty_array() : void
    {
        $options = new Options([]);

        self::assertSame([], $options->toArray());
        self::assertFalse($options->has('any-option'));
    }

    public function test_constructor_with_multiple_options() : void
    {
        $options = new Options([
            'content-type' => 'text/csv',
            'charset' => 'utf-8',
            'delimiter' => ',',
        ]);

        self::assertCount(3, $options->toArray());
        self::assertTrue($options->has('content-type'));
        self::assertTrue($options->has('charset'));
        self::assertTrue($options->has('delimiter'));
    }

    /**
     * @param array<array-key, mixed> $input
     * @param array<string, mixed> $expected
     */
    #[DataProvider('provider_constructor_with_various_input_types')]
    public function test_constructor_with_various_input_types(array $input, array $expected) : void
    {
        $options = new Options($input);

        self::assertSame($expected, $options->toArray());
    }

    #[DataProvider('provider_get_as_string_scenarios')]
    public function test_get_as_string_with_various_scenarios(string $option, ?string $default, ?string $expected) : void
    {
        $options = new Options(['content-type' => 'text/csv']);

        self::assertSame($expected, $options->getAsString($option, $default));
    }

    public function test_has_returns_false_for_non_existent_option() : void
    {
        $options = new Options(['content-type' => 'text/csv']);

        self::assertFalse($options->has('missing-option'));
        self::assertFalse($options->has('charset'));
        self::assertFalse($options->has(''));
    }

    public function test_has_returns_true_for_existing_option() : void
    {
        $options = new Options(['content-type' => 'text/csv']);

        self::assertTrue($options->has('content-type'));
    }

    public function test_set_adds_new_option() : void
    {
        $options = new Options(['initial' => 'value']);

        $options->set('new-option', 'new-value');

        self::assertTrue($options->has('new-option'));
        self::assertSame('new-value', $options->getAsString('new-option'));
    }

    public function test_set_overwrites_existing_option() : void
    {
        $options = new Options(['content-type' => 'text/csv']);

        $options->set('content-type', 'application/json');

        self::assertSame('application/json', $options->getAsString('content-type'));
    }

    /**
     * @param array<string, string> $initial
     * @param array<string, string> $expected
     */
    #[DataProvider('provider_set_when_empty_scenarios')]
    public function test_set_when_empty_with_various_scenarios(array $initial, string $option, string $value, array $expected) : void
    {
        $options = new Options($initial);

        $options->setWhenEmpty($option, $value);

        self::assertSame($expected, $options->toArray());
    }

    public function test_to_array_returns_all_options() : void
    {
        $inputOptions = [
            'content-type' => 'text/csv',
            'charset' => 'utf-8',
            'delimiter' => ',',
        ];

        $options = new Options($inputOptions);
        $result = $options->toArray();

        self::assertCount(3, $result);
        self::assertSame($inputOptions, $result);
    }

    public function test_to_array_returns_empty_array_when_no_options() : void
    {
        $options = new Options([]);

        self::assertSame([], $options->toArray());
    }

    public function test_to_array_returns_normalized_keys() : void
    {
        $options = new Options([
            'Content-Type' => 'text/csv',
            'CHARSET' => 'utf-8',
        ]);

        $result = $options->toArray();

        self::assertArrayHasKey('content-type', $result);
        self::assertArrayHasKey('charset', $result);
        self::assertArrayNotHasKey('Content-Type', $result);
        self::assertArrayNotHasKey('CHARSET', $result);
    }

    public function test_workflow_with_multiple_operations() : void
    {
        $options = new Options(['initial' => 'value']);

        $options->set('content-type', 'text/csv');
        $options->setWhenEmpty('charset', 'utf-8');
        $options->setWhenEmpty('content-type', 'application/json');

        self::assertSame('text/csv', $options->getAsString('content-type'));
        self::assertSame('utf-8', $options->getAsString('charset'));
        self::assertSame('value', $options->getAsString('initial'));

        $options->set('content-type', 'application/json');

        self::assertSame('application/json', $options->getAsString('content-type'));
        self::assertCount(3, $options->toArray());
    }
}
