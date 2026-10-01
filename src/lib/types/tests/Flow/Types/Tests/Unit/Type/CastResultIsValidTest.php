<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

/**
 * Container casts do not re-validate what their element casts return, so every cast result must be valid on its own.
 */
final class CastResultIsValidTest extends TestCase
{
    public static function types(): Generator
    {
        yield 'integer' => [type_integer()];
        yield 'float' => [type_float()];
        yield 'boolean' => [type_boolean()];
        yield 'string' => [type_string()];
        yield 'numeric_string' => [type_numeric_string()];
        yield 'date' => [type_date()];
        yield 'datetime Europe/Warsaw' => [type_datetime('Europe/Warsaw')];
        yield 'uuid' => [type_uuid()];
        yield 'json' => [type_json()];
        yield 'xml' => [type_xml()];
        yield 'optional<date>' => [type_optional(type_date())];
        yield 'list<date>' => [type_list(type_date())];
        yield 'list<?numeric_string>' => [type_list(type_optional(type_numeric_string()))];
        yield 'list<list<integer>>' => [type_list(type_list(type_integer()))];
        yield 'map<string, date>' => [type_map(type_string(), type_date())];
        yield 'map<integer, string>' => [type_map(type_integer(), type_string())];
        yield 'structure{a: date}' => [type_structure(['a' => type_date()])];
        yield 'structure{a: numeric_string, b?: numeric_string}' => [type_structure([
            'a' => type_numeric_string(),
            'b' => structure_element('b', type_numeric_string(), optional: true),
        ])];
        yield 'structure{a?: string}' => [type_structure(['a' => structure_element(
            'a',
            type_string(),
            optional: true,
        )])];
        yield 'list<structure{a: float}>' => [type_list(type_structure(['a' => type_float()]))];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('types')]
    public function test_every_cast_result_is_valid_for_its_type(Type $type): void
    {
        $warsaw = new DateTimeImmutable('2024-01-02 03:04:05.123456', new DateTimeZone('Europe/Warsaw'));
        $values = [
            null,
            true,
            0,
            5,
            1.5,
            NAN,
            INF,
            '',
            '5',
            'abc',
            '1e3',
            '2024-01-02',
            '2024-01-02T03:04:05Z',
            '0193f3a0-7b1c-7e6d-9d3a-1c2b3a4d5e6f',
            '{"a":1}',
            '<a/>',
            $warsaw,
            new DateTime('2024-01-02'),
            new Uuid('0193f3a0-7b1c-7e6d-9d3a-1c2b3a4d5e6f'),
            new Json('{"a":1}'),
            ['5'],
            ['a' => '2024-01-02'],
            [1 => 'x'],
            ['a' => $warsaw],
            [[1, 2]],
            ['a' => '1', 'b' => INF],
            [],
            [['a' => 1.5]],
            ['1' => 'a', '2' => 'b'],
            [null],
        ];

        foreach ($values as $index => $value) {
            try {
                // @mago-ignore analysis:mixed-assignment
                $cast = $type->cast($value);
            } catch (Throwable) {
                continue;
            }

            static::assertTrue(
                $type->isValid($cast),
                "value #{$index} cast into {$type->toString()} is not valid for it",
            );
        }
    }
}
