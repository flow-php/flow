<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\StringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_fingerprint_options;
use function Flow\ETL\DSL\schema_metadata;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function hash;
use function ini_get;
use function ini_set;
use function preg_quote;

use const INF;
use const NAN;

final class FingerprintTest extends FlowTestCase
{
    public static function provider_identity_changes(): Generator
    {
        yield 'column name' => [schema(int_schema('id')), schema(int_schema('ID'))];
        yield 'column order' => [
            schema(int_schema('id'), str_schema('name')),
            schema(str_schema('name'), int_schema('id')),
        ];
        yield 'column nullable' => [schema(int_schema('id')), schema(int_schema('id', nullable: true))];
        yield 'datetime zone' => [
            schema(datetime_schema('at', zone: 'UTC')),
            schema(datetime_schema('at', zone: 'Europe/Warsaw')),
        ];
        yield 'enum class' => [
            schema(enum_schema('e', BackedStringEnum::class)),
            schema(enum_schema('e', StringEnum::class)),
        ];
        yield 'list element optional' => [
            schema(list_schema('l', type_list(type_integer()))),
            schema(list_schema('l', type_list(type_optional(type_integer())))),
        ];
        yield 'structure field optional' => [
            schema(structure_schema('s', type_structure(['id' => structure_element('id', type_integer())]))),
            schema(structure_schema('s', type_structure(['id' => structure_element(
                'id',
                type_integer(),
                optional: true,
            )]))),
        ];
        yield 'structure field order' => [
            schema(structure_schema('s', type_structure(['a' => type_integer(), 'b' => type_string()]))),
            schema(structure_schema('s', type_structure(['b' => type_string(), 'a' => type_integer()]))),
        ];
        yield 'map value type' => [
            schema(map_schema('m', type_map(type_string(), type_integer()))),
            schema(map_schema('m', type_map(type_string(), type_string()))),
        ];
    }

    public function test_canonical_form_v1_is_pinned(): void
    {
        $fingerprint = schema(
            bool_schema('boolean'),
            int_schema('integer', nullable: true),
            float_schema('float'),
            str_schema('string'),
            definition_from_type('numeric_string', type_numeric_string()),
            date_schema('date'),
            datetime_schema('datetime', zone: 'Europe/Warsaw'),
            time_schema('time'),
            time_zone_schema('timezone'),
            json_schema('json'),
            uuid_schema('uuid'),
            enum_schema('enum', BackedStringEnum::class),
            list_schema('list', type_list(type_optional(type_integer()))),
            map_schema('map', type_map(type_string(), type_list(type_string()))),
            structure_schema('structure', type_structure([
                'id' => type_integer(),
                'note' => structure_element('note', type_optional(type_string()), optional: true),
            ])),
            html_schema('html'),
            html_element_schema('html_element'),
            xml_schema('xml'),
            xml_element_schema('xml_element'),
            null_schema('null'),
        )->fingerprint();

        static::assertSame(
            '{"version":"v1","options":{"metadata_keys":[]},"columns":[{"ref":"boolean","type":{"type":"boolean"},"nullable":false},{"ref":"integer","type":{"type":"integer"},"nullable":true},{"ref":"float","type":{"type":"float"},"nullable":false},{"ref":"string","type":{"type":"string"},"nullable":false},{"ref":"numeric_string","type":{"type":"numeric-string"},"nullable":false},{"ref":"date","type":{"type":"date"},"nullable":false},{"ref":"datetime","type":{"type":"datetime","zone":"Europe/Warsaw"},"nullable":false},{"ref":"time","type":{"type":"time"},"nullable":false},{"ref":"timezone","type":{"type":"timezone"},"nullable":false},{"ref":"json","type":{"type":"json"},"nullable":false},{"ref":"uuid","type":{"type":"uuid"},"nullable":false},{"ref":"enum","type":{"type":"enum","class":"Flow\\\\ETL\\\\Tests\\\\Fixtures\\\\Enum\\\\BackedStringEnum"},"nullable":false},{"ref":"list","type":{"type":"list","element":{"type":"optional","base":{"type":"integer"}}},"nullable":false},{"ref":"map","type":{"type":"map","key":{"type":"string"},"value":{"type":"list","element":{"type":"string"}}},"nullable":false},{"ref":"structure","type":{"type":"structure_v2","fields":[{"name":"id","type":{"type":"integer"},"optional":false},{"name":"note","type":{"type":"optional","base":{"type":"string"}},"optional":true}]},"nullable":false},{"ref":"html","type":{"type":"html"},"nullable":false},{"ref":"html_element","type":{"type":"html_element"},"nullable":false},{"ref":"xml","type":{"type":"xml"},"nullable":false},{"ref":"xml_element","type":{"type":"xml_element"},"nullable":false},{"ref":"null","type":{"type":"null"},"nullable":true}]}',
            $fingerprint->canonical(),
        );
        static::assertSame('3b7fe4c0aece8968761badc9be4a1ff2', $fingerprint->toString());
    }

    public function test_canonical_form_with_selected_metadata_is_pinned(): void
    {
        static::assertSame(
            '{"version":"v1","options":{"metadata_keys":["pii","tags","unit"]},"columns":[{"ref":"id","type":{"type":"integer"},"nullable":false,"metadata":{"pii":false,"tags":null,"unit":"ms"}},{"ref":"email","type":{"type":"string"},"nullable":false,"metadata":{"pii":true,"tags":{"a":[3,1],"b":1.0},"unit":null}}]}',
            schema(
                int_schema('id', metadata: schema_metadata(['pii' => false, 'unit' => 'ms', 'doc' => 'x'])),
                str_schema('email', metadata: schema_metadata(['pii' => true, 'tags' => ['b' => 1.0, 'a' => [3, 1]]])),
            )
                ->fingerprint(schema_fingerprint_options()->withMetadataKeys('unit', 'pii', 'tags', 'pii'))
                ->canonical(),
        );
    }

    public function test_metadata_is_excluded_by_default(): void
    {
        static::assertTrue(
            schema(int_schema('id', metadata: schema_metadata(['pii' => true])))
                ->fingerprint()
                ->equals(schema(int_schema('id', metadata: schema_metadata(['pii' => false])))->fingerprint()),
        );
    }

    public function test_requested_key_list_changes_fingerprint_without_any_column_holding_it(): void
    {
        $schema = schema(int_schema('id'));

        static::assertFalse(
            $schema->fingerprint()->equals($schema->fingerprint(schema_fingerprint_options()->withMetadataKeys('pii'))),
        );
    }

    /**
     * @param array<bool|float|int|string>|bool|int|string $value
     */
    #[TestWith([false])]
    #[TestWith([0])]
    #[TestWith([''])]
    #[TestWith([[]])]
    public function test_absent_key_differs_from_every_value(bool|int|string|array $value): void
    {
        $options = schema_fingerprint_options()->withMetadataKeys('pii');

        static::assertFalse(
            schema(int_schema('id'))
                ->fingerprint($options)
                ->equals(schema(int_schema('id', metadata: schema_metadata(['pii' => $value])))->fingerprint($options)),
        );
    }

    #[TestWith([1, '1'])]
    #[TestWith([1, 1.0])]
    #[TestWith([1, true])]
    #[TestWith(['1', 1.0])]
    #[TestWith(['1', true])]
    #[TestWith([1.0, true])]
    public function test_scalar_metadata_values_are_distinct(
        bool|int|float|string $left,
        bool|int|float|string $right,
    ): void {
        $options = schema_fingerprint_options()->withMetadataKeys('pii');

        static::assertFalse(
            schema(int_schema('id', metadata: schema_metadata(['pii' => $left])))
                ->fingerprint($options)
                ->equals(schema(int_schema('id', metadata: schema_metadata(['pii' => $right])))->fingerprint($options)),
        );
    }

    public function test_nested_map_order_does_not_change_fingerprint(): void
    {
        $options = schema_fingerprint_options()->withMetadataKeys('tags');

        static::assertTrue(
            schema(int_schema('id', metadata: schema_metadata(['tags' => ['b' => 1, 'a' => ['y' => 1, 'x' => 2]]])))
                ->fingerprint($options)
                ->equals(
                    schema(int_schema('id', metadata: schema_metadata([
                        'tags' => ['a' => ['x' => 2, 'y' => 1], 'b' => 1],
                    ])))
                        ->fingerprint($options),
                ),
        );
    }

    public function test_list_order_changes_fingerprint(): void
    {
        $options = schema_fingerprint_options()->withMetadataKeys('tags');

        static::assertFalse(
            schema(int_schema('id', metadata: schema_metadata(['tags' => [1, 3]])))
                ->fingerprint($options)
                ->equals(
                    schema(int_schema('id', metadata: schema_metadata(['tags' => [3, 1]])))->fingerprint($options),
                ),
        );
    }

    public function test_without_metadata_keys_serialize_precision_is_left_alone(): void
    {
        $precision = ini_get('serialize_precision');

        try {
            ini_set('serialize_precision', '17');

            $canonical = schema(float_schema('p', metadata: schema_metadata(['f' => 0.1])))->fingerprint()->canonical();

            static::assertSame('17', ini_get('serialize_precision'));
            static::assertStringNotContainsString('"metadata"', $canonical);
        } finally {
            ini_set('serialize_precision', (string) $precision);
        }
    }

    #[DataProvider('provider_identity_changes')]
    public function test_identity_changes(Schema $left, Schema $right): void
    {
        static::assertFalse($left->fingerprint()->equals($right->fingerprint()));
    }

    public function test_column_names_are_compared_as_bytes(): void
    {
        static::assertFalse(
            schema(int_schema("\u{00E9}"))->fingerprint()->equals(schema(int_schema("e\u{0301}"))->fingerprint()),
        );
    }

    public function test_float_encoding_ignores_serialize_precision(): void
    {
        $precision = ini_get('serialize_precision');

        try {
            ini_set('serialize_precision', '17');

            $canonical = schema(float_schema('p', metadata: schema_metadata(['f' => 0.1])))
                ->fingerprint(schema_fingerprint_options()->withMetadataKeys('f'))
                ->canonical();

            static::assertStringContainsString('"f":0.1', $canonical);
            static::assertSame('17', ini_get('serialize_precision'));
        } finally {
            ini_set('serialize_precision', (string) $precision);
        }
    }

    public function test_nan_in_selected_metadata_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches(
            '/^'
            . preg_quote('Schema fingerprint cannot encode column "price": Inf and NaN cannot be JSON encoded.', '/')
            . '/',
        );

        schema(
            int_schema('id'),
            float_schema('price', metadata: schema_metadata(['unit' => NAN])),
        )->fingerprint(schema_fingerprint_options()->withMetadataKeys('unit'));
    }

    public function test_infinity_in_selected_metadata_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches(
            '/^'
            . preg_quote('Schema fingerprint cannot encode column "price": Inf and NaN cannot be JSON encoded.', '/')
            . '/',
        );

        schema(
            int_schema('id'),
            float_schema('price', metadata: schema_metadata(['unit' => INF])),
        )->fingerprint(schema_fingerprint_options()->withMetadataKeys('unit'));
    }

    public function test_nan_in_unselected_metadata_does_not_throw(): void
    {
        static::assertStringNotContainsString(
            '"metadata"',
            schema(float_schema('price', metadata: schema_metadata(['unit' => NAN])))->fingerprint()->canonical(),
        );
    }

    public function test_invalid_utf8_column_name_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches(
            '/^'
            . preg_quote(
                "Schema fingerprint cannot encode column \"\xB1\": Malformed UTF-8 characters, possibly incorrectly encoded.",
                '/',
            )
            . '/',
        );

        schema(int_schema("\xB1"))->fingerprint();
    }

    public function test_invalid_utf8_metadata_key_with_columns_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches(
            '/^'
            . preg_quote('Schema fingerprint cannot encode the selected metadata keys: Malformed UTF-8 characters', '/')
            . '/',
        );

        schema(int_schema('id'))->fingerprint(schema_fingerprint_options()->withMetadataKeys("\xB1"));
    }

    public function test_hash_uses_the_given_algorithm(): void
    {
        $fingerprint = schema(int_schema('id'))->fingerprint();

        static::assertSame(hash('sha256', $fingerprint->canonical()), $fingerprint->hash(HashAlgorithm::sha256));
    }

    public function test_to_string_is_the_default_xxh128_hash(): void
    {
        $fingerprint = schema(int_schema('id'))->fingerprint();

        static::assertSame(hash('xxh128', $fingerprint->canonical()), $fingerprint->toString());
    }

    public function test_equal_schemas_have_equal_fingerprints(): void
    {
        $left = schema(int_schema('id'), str_schema('email', nullable: true))->fingerprint();
        $right = schema(int_schema('id'), str_schema('email', nullable: true))->fingerprint();

        static::assertTrue($left->equals($right));
        static::assertSame($left->toString(), $right->toString());
    }
}
