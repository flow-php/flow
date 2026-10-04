<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use DOMElement;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\CastingException;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\cast;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function is_object;

final class CastTest extends FlowTestCase
{
    public function test_cast_of_a_null_value_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            cast(ref('value'), 'int'),
            [
                'value' => null,
            ],
            schema(str_schema('value', nullable: true)),
        );
    }

    public function test_casting_a_datetime_column_to_string_equals_casting_each_value(): void
    {
        $rows = [
            ['at' => new DateTimeImmutable('2026-03-29T00:59:59.123456Z')],
            ['at' => new DateTimeImmutable('2026-03-29T01:00:00Z')],
        ];

        foreach (['UTC', 'Europe/Warsaw'] as $zone) {
            static::assertSame(
                [
                    type_string()->cast($rows[0]['at']->setTimezone(new DateTimeZone($zone))),
                    type_string()->cast($rows[1]['at']->setTimezone(new DateTimeZone($zone))),
                ],
                (new FunctionContext(flow_context()))
                    ->column(cast(ref('at'), 'string'), $rows, schema(datetime_schema('at', zone: $zone)))
                    ->values(),
            );
        }
    }

    public function test_casting_a_date_column_to_string(): void
    {
        static::assertSame(
            ['2026-01-02T00:00:00+00:00', null],
            (new FunctionContext(flow_context()))
                ->column(
                    cast(ref('on'), type_optional(type_string())),
                    [['on' => new DateTimeImmutable('2026-01-02')], ['on' => null]],
                    schema(date_schema('on', nullable: true)),
                )
                ->values(),
        );
    }

    public function test_casting_a_null_datetime_to_string_is_refused_with_its_row_index(): void
    {
        try {
            (new FunctionContext(flow_context()))->column(
                cast(ref('at'), 'string'),
                [['at' => new DateTimeImmutable('2026-01-02')], ['at' => null]],
                schema(datetime_schema('at', nullable: true)),
            );
            static::fail('a null datetime was cast to a non-nullable string');
        } catch (EvaluationException $e) {
            static::assertSame(1, $e->rowIndex);
            static::assertInstanceOf(InvalidArgumentException::class, $e->getPrevious());
            static::assertSame('Cast function requires non-null value', $e->getPrevious()->getMessage());
        }
    }

    public function test_cast_of_a_null_value_to_an_optional_target_is_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            cast(ref('value'), type_optional(type_integer())),
            [
                'value' => null,
            ],
            schema(str_schema('value', nullable: true)),
        ));
    }

    public function test_a_refused_cast_keeps_the_casting_error_as_the_cause(): void
    {
        try {
            (new FunctionContext(flow_context()))->eval(
                cast(ref('value'), type_optional(type_datetime())),
                ['value' => 'abc'],
                schema(str_schema('value')),
            );
            static::fail('An unconvertible value must be refused.');
        } catch (EvaluationException $e) {
            static::assertInstanceOf(CastingException::class, $e->getPrevious()?->getPrevious());
        }
    }

    public function test_casting_an_unconvertible_value_to_an_optional_target_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "string" into "datetime" type');

        (new FunctionContext(flow_context()))->eval(
            cast(ref('value'), type_optional(type_datetime())),
            [
                'value' => 'abc',
            ],
            schema(str_schema('value')),
        );
    }

    public function test_constructor_rejects_a_non_representable_target(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function does not support type: object');

        cast(ref('value'), 'object');
    }

    public function test_double_and_real_resolve_to_float(): void
    {
        static::assertSame(1.0, (new FunctionContext(flow_context()))->eval(
            cast(ref('value'), 'double'),
            [
                'value' => '1',
            ],
            schema(str_schema('value')),
        ));
        static::assertSame(1.0, (new FunctionContext(flow_context()))->eval(
            cast(ref('value'), 'real'),
            [
                'value' => '1',
            ],
            schema(str_schema('value')),
        ));
    }

    public function test_constructor_rejects_json_pretty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function does not support type: json_pretty');

        cast(ref('value'), 'json_pretty');
    }

    /**
     * @return Generator<string, array{Type<mixed>, string}>
     */
    public static function union_targets(): Generator
    {
        yield 'union' => [type_union(type_integer(), type_string()), 'integer|string'];
        yield 'union nested in a list' => [
            type_list(type_union(type_integer(), type_string())),
            'list<integer|string>',
        ];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('union_targets')]
    public function test_constructor_rejects_a_union_target(Type $type, string $described): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function does not support type: ' . $described);

        cast(ref('value'), $type);
    }

    /**
     * @return array<string, array<int, mixed>>
     */
    public static function cast_provider(): array
    {
        $xml = new DOMDocument();
        $xml->loadXML($xmlString = '<root><foo baz="buz">bar</foo></root>');

        $node = new DOMDocument('1.0', 'UTF-8');
        $node->appendChild(new DOMElement('row', 'zażółć'));

        $fullXMLString = <<<'XML'
            <?xml version="1.0"?>
            <root><foo baz="buz">bar</foo></root>

            XML;

        return [
            'int' => ['1', str_schema('value'), 'int', 1],
            'integer' => ['1', str_schema('value'), 'integer', 1],
            'float' => ['1', str_schema('value'), 'float', 1.0],
            'double' => ['1', str_schema('value'), 'double', 1.0],
            'real' => ['1', str_schema('value'), 'real', 1.0],
            'string' => [1, int_schema('value'), 'string', '1'],
            'bool' => ['1', str_schema('value'), 'bool', true],
            'boolean' => ['1', str_schema('value'), 'boolean', true],
            'json' => [[1], list_schema('value', type_list(type_integer())), 'json', new Json('[1]')],
            'xml_to_array' => [
                $xml,
                xml_schema('value'),
                'array',
                ['root' => ['foo' => ['@attributes' => ['baz' => 'buz'], '@value' => 'bar']]],
            ],
            'string_to_xml' => [$xmlString, str_schema('value'), 'xml', $xml],
            'xml_to_string' => [$xml, xml_schema('value'), 'string', '<root><foo baz="buz">bar</foo></root>'],
            'xml_node_document_to_string' => [$node, xml_schema('value'), 'string', '<row>zażółć</row>'],
            'full_xml_to_string' => [$fullXMLString, str_schema('value'), 'string', $fullXMLString],
            'datetime' => [
                new DateTimeImmutable('2023-01-01 00:00:00 UTC'),
                datetime_schema('value'),
                'string',
                '2023-01-01T00:00:00+00:00',
            ],
            'datetime_to_date' => [
                new DateTimeImmutable('2023-01-01 00:01:00 UTC'),
                datetime_schema('value'),
                'date',
                new DateTimeImmutable('2023-01-01T00:00:00+00:00'),
            ],
            'string_to_timezone' => ['UTC', str_schema('value'), 'timezone', new DateTimeZone('UTC')],
            'string_to_timezone_america' => [
                'America/New_York',
                str_schema('value'),
                'timezone',
                new DateTimeZone('America/New_York'),
            ],
            // a UTC datetime column, so the value reads back under the column zone
            'datetime_to_timezone' => [
                new DateTimeImmutable('2023-01-01 00:00:00', new DateTimeZone('Europe/London')),
                datetime_schema('value'),
                'timezone',
                new DateTimeZone('UTC'),
            ],
            'uuid' => [
                Uuid::fromString('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
                uuid_schema('value'),
                'string',
                'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
            ],
            'bool_to_string' => [true, bool_schema('value'), 'string', 'true'],
        ];
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('cast_provider')]
    public function test_cast(mixed $from, Definition $definition, string $to, mixed $expected): void
    {
        // @mago-ignore analysis:mixed-assignment
        $resultRefCast = (new FunctionContext(flow_context()))->eval(
            ref('value')->cast($to),
            ['value' => $from],
            schema($definition),
        );
        // @mago-ignore analysis:mixed-assignment
        $resultCastRef = (new FunctionContext(flow_context()))->eval(
            cast(ref('value'), $to),
            ['value' => $from],
            schema($definition),
        );

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

        (new FunctionContext(flow_context()))->eval(
            ref('value')->cast('array'),
            ['value' => '1'],
            schema(str_schema('value')),
        );
    }

    public function test_casting_integer_to_timezone(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "int" into "timezone" type');

        (new FunctionContext(flow_context()))->eval(
            ref('value')->cast('timezone'),
            ['value' => 123],
            schema(int_schema('value')),
        );
    }

    public function test_casting_integer_to_xml(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "int" into "xml" type');

        (new FunctionContext(flow_context()))->eval(
            ref('value')->cast('xml'),
            ['value' => 1],
            schema(int_schema('value')),
        );
    }

    public function test_casting_invalid_string_to_timezone(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "string" into "timezone" type');

        (new FunctionContext(flow_context()))->eval(
            ref('value')->cast('timezone'),
            ['value' => 'invalid-timezone'],
            schema(str_schema('value')),
        );
    }

    public function test_casting_non_xml_string_to_xml(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "string" into "xml" type');

        (new FunctionContext(flow_context()))->eval(
            ref('value')->cast('xml'),
            ['value' => 'foo'],
            schema(str_schema('value')),
        );
    }
}
