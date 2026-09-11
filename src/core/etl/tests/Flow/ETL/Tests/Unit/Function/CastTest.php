<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\cast;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function is_object;

final class CastTest extends FlowTestCase
{
    public function test_cast_of_a_null_value_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function requires non-null value');

        cast(ref('value'), 'int')->eval(row(['value' => null]), flow_context());
    }

    public function test_constructor_rejects_a_non_representable_target(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function does not support type: object');

        cast(ref('value'), 'object');
    }

    public function test_double_and_real_resolve_to_float(): void
    {
        static::assertSame(1.0, cast(ref('value'), 'double')->eval(row(['value' => '1']), flow_context()));
        static::assertSame(1.0, cast(ref('value'), 'real')->eval(row(['value' => '1']), flow_context()));
    }

    public function test_constructor_rejects_json_pretty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function does not support type: json_pretty');

        cast(ref('value'), 'json_pretty');
    }

    /**
     * @return array<string, array<int, mixed>>
     */
    public static function cast_provider(): array
    {
        $xml = new DOMDocument();
        $xml->loadXML($xmlString = '<root><foo baz="buz">bar</foo></root>');

        $fullXMLString = <<<'XML'
            <?xml version="1.0"?>
            <root><foo baz="buz">bar</foo></root>

            XML;

        return [
            'int' => ['1', 'int', 1],
            'integer' => ['1', 'integer', 1],
            'float' => ['1', 'float', 1.0],
            'double' => ['1', 'double', 1.0],
            'real' => ['1', 'real', 1.0],
            'string' => [1, 'string', '1'],
            'bool' => ['1', 'bool', true],
            'boolean' => ['1', 'boolean', true],
            'json' => [[1], 'json', new Json('[1]')],
            'xml_to_array' => [
                $xml,
                'array',
                ['root' => ['foo' => ['@attributes' => ['baz' => 'buz'], '@value' => 'bar']]],
            ],
            'string_to_xml' => [$xmlString, 'xml', $xml],
            'xml_to_string' => [$xml, 'string', '<root><foo baz="buz">bar</foo></root>'],
            'full_xml_to_string' => [$fullXMLString, 'string', $fullXMLString],
            'datetime' => [new DateTimeImmutable('2023-01-01 00:00:00 UTC'), 'string', '2023-01-01T00:00:00+00:00'],
            'datetime_to_date' => [
                new DateTimeImmutable('2023-01-01 00:01:00 UTC'),
                'date',
                new DateTimeImmutable('2023-01-01T00:00:00+00:00'),
            ],
            'string_to_timezone' => ['UTC', 'timezone', new DateTimeZone('UTC')],
            'string_to_timezone_america' => ['America/New_York', 'timezone', new DateTimeZone('America/New_York')],
            'datetime_to_timezone' => [
                new DateTimeImmutable('2023-01-01 00:00:00', new DateTimeZone('Europe/London')),
                'timezone',
                new DateTimeZone('Europe/London'),
            ],
            'uuid' => [
                Uuid::fromString('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
                'string',
                'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
            ],
            'bool_to_string' => [true, 'string', 'true'],
        ];
    }

    #[DataProvider('cast_provider')]
    public function test_cast(mixed $from, string $to, mixed $expected): void
    {
        // @mago-ignore analysis:mixed-assignment
        $resultRefCast = ref('value')->cast($to)->eval(row(['value' => $from]), flow_context());
        // @mago-ignore analysis:mixed-assignment
        $resultCastRef = cast(ref('value'), $to)->eval(row(['value' => $from]), flow_context());

        if (is_object($expected) || is_object($from)) {
            static::assertEquals($expected, $resultRefCast);
            static::assertEquals($expected, $resultCastRef);
        } else {
            static::assertSame($expected, $resultRefCast);
            static::assertSame($expected, $resultCastRef);
        }
    }

    public function test_casting_a_scalar_to_array_names_the_remedy(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Cast function failed: Can\'t cast "string" into "array<mixed>" type: wrap the value first, e.g. type_list(type_string())',
        );

        ref('value')->cast('array')->eval(row(['value' => '1']), flow_context());
    }

    public function test_casting_integer_to_timezone(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "int" into "timezone" type');

        ref('value')->cast('timezone')->eval(row(['value' => 123]), flow_context());
    }

    public function test_casting_integer_to_xml(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "int" into "xml" type');

        ref('value')->cast('xml')->eval(row(['value' => 1]), flow_context());
    }

    public function test_casting_invalid_string_to_timezone(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "string" into "timezone" type');

        ref('value')->cast('timezone')->eval(row(['value' => 'invalid-timezone']), flow_context());
    }

    public function test_casting_non_xml_string_to_xml(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "string" into "xml" type');

        ref('value')->cast('xml')->eval(row(['value' => 'foo']), flow_context());
    }
}
