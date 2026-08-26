<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\RustRowHydratorNative;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use Throwable;

use function class_exists;
use function extension_loaded;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_entry;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_non_empty_string;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;
use function serialize;

final class NativeRowHydratorTest extends FlowTestCase
{
    /**
     * @return \Generator<string, array{Schema, list<RawRowValues>}>
     */
    public static function serializable_datasets(): Generator
    {
        /** @var UnionType<mixed, mixed> $union */
        $union = type_union(type_string(), type_integer());

        yield 'union column across members, null and absent' => [
            schema(int_schema('id'), new UnionDefinition('a', $union, true)),
            [
                new RawRowValues(['id' => 1, 'a' => 42]),
                new RawRowValues(['id' => 2, 'a' => 'x']),
                new RawRowValues(['id' => 3, 'a' => null]),
                new RawRowValues(['id' => 4]),
            ],
        ];

        yield 'scalars over multiple rows' => [
            schema(int_schema('id'), str_schema('name', nullable: true), float_schema('p'), bool_schema('a')),
            [
                new RawRowValues(['id' => 1, 'name' => 'x', 'p' => 1.5, 'a' => true]),
                new RawRowValues(['id' => 2, 'name' => null, 'p' => -0.25, 'a' => false]),
                new RawRowValues(['id' => 3, 'name' => 'z', 'p' => 0.0, 'a' => true]),
            ],
        ];

        yield 'null on a non-nullable column across rows' => [
            schema(int_schema('id'), str_schema('name')),
            [
                new RawRowValues(['id' => 1, 'name' => null]),
                new RawRowValues(['id' => 2, 'name' => null]),
            ],
        ];

        yield 'absent columns are skipped' => [
            schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('flag', nullable: true)),
            [
                new RawRowValues(['id' => 1]),
                new RawRowValues(['id' => 2, 'name' => 'y']),
            ],
        ];

        yield 'per-value metadata divergence across rows' => [
            schema(int_schema('id')),
            [
                new RawRowValues(['id' => 1], ['id' => Metadata::fromArray(['k' => 'v1'])]),
                new RawRowValues(['id' => 2], ['id' => Metadata::fromArray(['k' => 'v2'])]),
            ],
        ];

        yield 'per-value metadata together with a null non-nullable value' => [
            schema(int_schema('id')),
            [new RawRowValues(['id' => null], ['id' => Metadata::fromArray(['k' => 'v'])])],
        ];

        yield 'temporal and uuid values' => [
            schema(datetime_schema('at'), date_schema('d'), time_schema('t'), uuid_schema('u')),
            [new RawRowValues([
                'at' => new DateTimeImmutable('2025-01-01 12:00:00.123456', new DateTimeZone('Europe/Warsaw')),
                'd' => new DateTimeImmutable('2025-03-01'),
                't' => new DateInterval('PT2H30M5S'),
                'u' => new Uuid('01234567-89ab-4def-8123-456789abcdef'),
            ])],
        ];

        yield 'containers and json' => [
            schema(
                list_schema('l', type_list(type_integer())),
                map_schema('m', type_map(type_string(), type_integer())),
                structure_schema('st', type_structure(['a' => type_integer(), 'b' => type_string()])),
                list_schema('opt', type_list(type_optional(type_integer()))),
                json_schema('j'),
            ),
            [new RawRowValues([
                'l' => [1, 2, 3],
                'm' => ['a' => 1],
                'st' => ['a' => 1, 'b' => 'kept'],
                'opt' => [1, null],
                'j' => Json::fromArray(['x' => 1]),
            ])],
        ];

        yield 'enum values including a null variant' => [
            schema(enum_schema('s', ParitySuit::class)),
            [
                new RawRowValues(['s' => ParitySuit::Hearts]),
                new RawRowValues(['s' => ParitySuit::Spades]),
                new RawRowValues(['s' => null]),
            ],
        ];

        yield 'null definition column' => [
            schema(null_schema('n'), int_schema('id', nullable: true)),
            [
                new RawRowValues(['n' => null, 'id' => null]),
                new RawRowValues(['n' => null, 'id' => 5]),
            ],
        ];

        yield 'empty batch' => [schema(int_schema('id')), []];
    }

    /**
     * @return \Generator<string, array{Schema, list<RawRowValues>}>
     */
    public static function castable_datasets(): Generator
    {
        /** @var UnionType<mixed, mixed> $union */
        $union = type_union(type_string(), type_integer());

        yield 'union column across members, null and absent' => [
            schema(int_schema('id'), new UnionDefinition('a', $union, true)),
            [
                new RawRowValues(['id' => 1, 'a' => 42]),
                new RawRowValues(['id' => 2, 'a' => 'x']),
                new RawRowValues(['id' => 3, 'a' => null]),
                new RawRowValues(['id' => 4]),
                new RawRowValues(['id' => 5, 'a' => '42']),
                new RawRowValues(['id' => 6, 'a' => 1.5]),
            ],
        ];

        yield 'union column with per-value metadata' => [
            schema(new UnionDefinition('a', $union, true)),
            [
                new RawRowValues(['a' => 42], ['a' => Metadata::fromArray(['k' => 'v'])]),
                new RawRowValues(['a' => 'x'], ['a' => Metadata::fromArray(['k' => 'v'])]),
            ],
        ];

        yield 'scalar columns from raw strings and coercion edges' => [
            schema(int_schema('id'), float_schema('price'), bool_schema('active'), str_schema('name')),
            [
                new RawRowValues(['id' => '42', 'price' => '3.14', 'active' => 'yes', 'name' => 7]),
                new RawRowValues(['id' => ' 7', 'price' => '1e3', 'active' => 'OFF', 'name' => 1.5]),
                new RawRowValues(['id' => 'abc', 'price' => '0x1A', 'active' => 'weird', 'name' => true]),
                new RawRowValues(['id' => '12abc', 'price' => true, 'active' => 0, 'name' => false]),
                new RawRowValues(['id' => '9223372036854775808', 'price' => 7, 'active' => 3.5, 'name' => null]),
                new RawRowValues(['id' => 5, 'price' => 2.5, 'active' => true, 'name' => 'text']),
            ],
        ];

        yield 'nested positive integer and string family elements' => [
            schema(
                list_schema('counts', type_list(type_positive_integer())),
                structure_schema('labels', type_structure(['label' => type_non_empty_string()])),
                structure_schema('codes', type_structure(['code' => type_numeric_string()])),
            ),
            [
                new RawRowValues([
                    'counts' => [5, 7],
                    'labels' => ['label' => 'x'],
                    'codes' => ['code' => '42'],
                ]),
                new RawRowValues([
                    'counts' => ['5', 8],
                    'labels' => ['label' => 9],
                    'codes' => ['code' => 42],
                ]),
                new RawRowValues([
                    'counts' => [],
                    'labels' => ['label' => 1.5],
                    'codes' => ['code' => '3.14'],
                ]),
                new RawRowValues([
                    'counts' => [1],
                    'labels' => ['label' => true],
                    'codes' => ['code' => 42.5],
                ]),
            ],
        ];

        $shared = new DateTimeImmutable('2025-01-01 12:00:00.123456', new DateTimeZone('Europe/Warsaw'));

        yield 'temporal columns preserving object identity topology' => [
            schema(datetime_schema('at'), datetime_schema('at2'), date_schema('d')),
            [
                new RawRowValues(['at' => $shared, 'at2' => $shared, 'd' => $shared]),
                new RawRowValues([
                    'at' => $shared,
                    'at2' => '2024-03-01 10:20:30',
                    'd' => new DateTimeImmutable('2025-03-01'),
                ]),
                new RawRowValues(['at' => 1700000000, 'at2' => 1700000000.5, 'd' => '2024-03-05 08:30:00']),
                new RawRowValues(['at' => '2024-02-29T12:00:00+02:00', 'at2' => '2024-06-01', 'd' => 1700000000]),
            ],
        ];

        yield 'uuid and json columns from raw values' => [
            schema(uuid_schema('u'), json_schema('j')),
            [
                new RawRowValues(['u' => '01234567-89ab-4def-8123-456789abcdef', 'j' => '["a","b"]']),
                new RawRowValues(['u' => new Uuid('01234567-89ab-4def-8123-456789abcdef'), 'j' => '{"a":1}']),
                new RawRowValues(['u' => null, 'j' => ['a' => 1]]),
                new RawRowValues(['u' => '11234567-89ab-4def-8123-456789abcdef', 'j' => []]),
                new RawRowValues(['u' => '21234567-89ab-4def-8123-456789abcdef', 'j' => Json::fromArray(['x' => 1])]),
            ],
        ];

        /** @var StructureType<array<array-key, mixed>> $allowExtraStructure */
        $allowExtraStructure = type_structure(['a' => type_integer()], ['b' => type_string()], true);

        yield 'containers from raw values' => [
            schema(
                list_schema('l', type_list(type_integer())),
                list_schema('ll', type_list(type_list(type_integer()))),
                map_schema('m', type_map(type_string(), type_integer())),
                map_schema('mi', type_map(type_integer(), type_string())),
                list_schema('lo', type_list(type_optional(type_integer()))),
                structure_schema('st', type_structure(['a' => type_integer()], ['b' => type_string()])),
                structure_schema('se', $allowExtraStructure),
            ),
            [
                new RawRowValues([
                    'l' => ['1', 2, '3'],
                    'll' => [['1', '2'], [3]],
                    'm' => ['a' => '1', 'b' => 2],
                    'mi' => [0 => 'x', 5 => 7],
                    'lo' => ['1', null, 3],
                    'st' => ['a' => '5', 'extra' => 'dropped'],
                    'se' => ['a' => 1, 'b' => 'kept', 'other' => 'dropped'],
                ]),
                new RawRowValues([
                    'l' => [],
                    'll' => [],
                    'm' => [],
                    'mi' => [],
                    'lo' => [],
                    'st' => ['a' => 1, 'b' => 'present'],
                    'se' => ['a' => 2],
                ]),
            ],
        ];

        yield 'exotic columns cast through the per-value fallback' => [
            schema(enum_schema('s', ParitySuit::class), xml_schema('x'), time_schema('t')),
            [
                new RawRowValues([
                    's' => ParitySuit::Hearts,
                    'x' => '<root a="1"><i>v</i></root>',
                    't' => new DateInterval('PT2H30M5S'),
                ]),
                new RawRowValues(['s' => 'h', 'x' => '<other/>', 't' => 'PT2H']),
            ],
        ];

        yield 'metadata variants fill-missing and extra raw keys' => [
            schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('flag')),
            [
                new RawRowValues(['id' => '1'], ['id' => Metadata::fromArray(['k' => 'v1'])]),
                new RawRowValues([]),
                new RawRowValues(['id' => 2, 'name' => null, 'unknown' => 'dropped', 'flag' => 'on']),
                new RawRowValues(['id' => null], ['id' => Metadata::fromArray(['k' => 'v2'])]),
            ],
        ];

        yield 'empty cast batch' => [schema(int_schema('id')), []];

        yield 'all-optional structure with no matching keys' => [
            schema(structure_schema('st', type_structure([], ['b' => type_string()]))),
            [new RawRowValues(['st' => ['other' => 1]])],
        ];
    }

    /**
     * @return \Generator<string, array{Schema, list<RawRowValues>}>
     */
    public static function throwing_cast_datasets(): Generator
    {
        /** @var UnionType<mixed, mixed> $unmatchable */
        $unmatchable = type_union(type_uuid(), type_datetime());

        yield 'union column with a value outside every member' => [
            schema(new UnionDefinition('a', $unmatchable)),
            [new RawRowValues(['a' => [1, 2]])],
        ];

        yield 'invalid uuid string' => [
            schema(uuid_schema('u')),
            [new RawRowValues(['u' => 'not-a-uuid'])],
        ];

        yield 'uppercase uuid string' => [
            schema(uuid_schema('u')),
            [new RawRowValues(['u' => '01234567-89AB-4DEF-8123-456789ABCDEF'])],
        ];

        yield 'json from a scalar' => [
            schema(json_schema('j')),
            [new RawRowValues(['j' => 5])],
        ];

        yield 'json from an invalid json string' => [
            schema(json_schema('j')),
            [new RawRowValues(['j' => '{oops'])],
        ];

        yield 'json from a plain string' => [
            schema(json_schema('j')),
            [new RawRowValues(['j' => 'plain'])],
        ];

        yield 'datetime from garbage' => [
            schema(datetime_schema('at')),
            [new RawRowValues(['at' => 'not-a-date'])],
        ];

        yield 'datetime from an array' => [
            schema(datetime_schema('at')),
            [new RawRowValues(['at' => ['nope']])],
        ];

        yield 'date from garbage' => [
            schema(date_schema('d')),
            [new RawRowValues(['d' => 'not-a-date'])],
        ];

        yield 'string map with integer keys' => [
            schema(map_schema('m', type_map(type_string(), type_integer()))),
            [new RawRowValues(['m' => [5 => 1]])],
        ];

        yield 'list with non-sequential keys' => [
            schema(list_schema('l', type_list(type_integer()))),
            [new RawRowValues(['l' => [1 => 'x']])],
        ];

        yield 'positive integer list from a non-numeric string' => [
            schema(list_schema('l', type_list(type_positive_integer()))),
            [new RawRowValues(['l' => ['abc']])],
        ];

        yield 'positive integer list from a negative int' => [
            schema(list_schema('l', type_list(type_positive_integer()))),
            [new RawRowValues(['l' => [-3]])],
        ];

        yield 'structure missing required element' => [
            schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            [new RawRowValues(['data' => ['id' => 1]])],
        ];

        yield 'structure present-null required element' => [
            schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            [new RawRowValues(['data' => ['id' => 1, 'name' => null]])],
        ];

        yield 'structure present-null optional element' => [
            schema(structure_schema('data', type_structure(['id' => type_integer()], ['name' => type_string()]))),
            [new RawRowValues(['data' => ['id' => 1, 'name' => null]])],
        ];
    }

    /**
     * @param list<RawRowValues> $batch
     */
    #[DataProvider('serializable_datasets')]
    public function test_native_hydrate_is_serialize_identical_to_php(Schema $schema, array $batch): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        static::assertSame(
            serialize((new PhpRowHydrator())->hydrate($batch, $schema)),
            serialize((new NativeRowHydrator())->hydrate($batch, $schema)),
        );
    }

    /**
     * @param list<RawRowValues> $batch
     */
    #[DataProvider('serializable_datasets')]
    public function test_native_dehydrate_is_serialize_identical_to_php(Schema $schema, array $batch): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        $php = new PhpRowHydrator();
        $rows = $php->hydrate($batch, $schema);

        static::assertSame(serialize($php->dehydrate($rows)), serialize((new NativeRowHydrator())->dehydrate($rows)));
    }

    /**
     * @param list<RawRowValues> $batch
     */
    #[DataProvider('castable_datasets')]
    public function test_native_cast_is_serialize_identical_to_php(Schema $schema, array $batch): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        static::assertSame(
            serialize((new PhpRowHydrator())->cast($batch, $schema)),
            serialize((new NativeRowHydrator())->cast($batch, $schema)),
        );
    }

    /**
     * @param list<RawRowValues> $batch
     */
    #[DataProvider('throwing_cast_datasets')]
    public function test_native_cast_exception_parity(Schema $schema, array $batch): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        $phpException = null;

        try {
            (new PhpRowHydrator())->cast($batch, $schema);
        } catch (Throwable $e) {
            $phpException = $e;
        }

        $nativeException = null;

        try {
            (new NativeRowHydrator())->cast($batch, $schema);
        } catch (Throwable $e) {
            $nativeException = $e;
        }

        static::assertNotNull($phpException);
        static::assertNotNull($nativeException);
        static::assertSame($phpException::class, $nativeException::class);
        static::assertSame($phpException->getMessage(), $nativeException->getMessage());
    }

    /**
     * @param list<RawRowValues> $batch
     */
    #[DataProvider('castable_datasets')]
    public function test_native_cast_is_idempotent_on_already_cast_values(Schema $schema, array $batch): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        $php = new PhpRowHydrator();
        $recastBatch = [];

        foreach ($php->cast($batch, $schema) as $row) {
            $values = [];

            foreach ($row->entries()->all() as $entry) {
                $values[$entry->name()] = $entry->value();
            }

            $recastBatch[] = new RawRowValues($values);
        }

        static::assertSame(
            serialize($php->cast($recastBatch, $schema)),
            serialize((new NativeRowHydrator())->cast($recastBatch, $schema)),
        );
    }

    public function test_native_cast_without_schema_delegates_to_php_inference(): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        $batch = [
            new RawRowValues(['id' => 1, 'name' => 'a', 'price' => 1.5]),
            new RawRowValues(['id' => 2, 'name' => null, 'price' => 0.25]),
        ];

        static::assertSame(
            serialize((new PhpRowHydrator())->cast($batch)),
            serialize((new NativeRowHydrator())->cast($batch)),
        );
    }

    public function test_native_cast_follows_schema_changes(): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        $schema = schema(int_schema('id'), str_schema('name'));
        $php = new PhpRowHydrator();
        $native = new NativeRowHydrator();

        $batch = [new RawRowValues(['id' => '1', 'name' => 7])];
        static::assertSame(serialize($php->cast($batch, $schema)), serialize($native->cast($batch, $schema)));

        $schema = $schema->add(bool_schema('active', nullable: true));

        $batch = [new RawRowValues(['id' => '2', 'name' => 'b', 'active' => 'yes'])];
        static::assertSame(serialize($php->cast($batch, $schema)), serialize($native->cast($batch, $schema)));

        $schema = $schema->makeNullable();

        $batch = [new RawRowValues(['id' => null, 'name' => null, 'active' => null])];
        static::assertSame(serialize($php->cast($batch, $schema)), serialize($native->cast($batch, $schema)));
    }

    public function test_native_moves_markup_values_verbatim(): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        $document = new DOMDocument();
        $document->loadXML('<root a="1"><i>v</i></root>');

        $rows = rows(row(xml_entry('doc', $document)));

        $php = new PhpRowHydrator();
        $native = new NativeRowHydrator();

        $phpDehydrated = $php->dehydrate($rows);
        $nativeDehydrated = $native->dehydrate($rows);

        static::assertSame($phpDehydrated[0]->values['doc'], $nativeDehydrated[0]->values['doc']);
        static::assertEquals($phpDehydrated[0]->types['doc'], $nativeDehydrated[0]->types['doc']);
    }

    public function test_native_hydrate_follows_schema_changes(): void
    {
        if (!NativeRowHydrator::isSupported()) {
            static::markTestSkipped('flow_php extension with the native hydrator is not loaded.');
        }

        $schema = schema(int_schema('id'), str_schema('name'));
        $php = new PhpRowHydrator();
        $native = new NativeRowHydrator();

        $batch = [new RawRowValues(['id' => 1, 'name' => 'a'])];
        static::assertSame(serialize($php->hydrate($batch, $schema)), serialize($native->hydrate($batch, $schema)));

        $schema = $schema->add(bool_schema('active', nullable: true));

        $batch = [new RawRowValues(['id' => 2, 'name' => 'b', 'active' => true])];
        static::assertSame(serialize($php->hydrate($batch, $schema)), serialize($native->hydrate($batch, $schema)));

        $schema = $schema->makeNullable();

        $batch = [new RawRowValues(['id' => null, 'name' => null, 'active' => null])];
        static::assertSame(serialize($php->hydrate($batch, $schema)), serialize($native->hydrate($batch, $schema)));
    }

    public function test_supports_extension_requires_the_native_class(): void
    {
        static::assertSame(
            extension_loaded('flow_php') && class_exists(RustRowHydratorNative::class, false),
            NativeRowHydrator::isSupported(),
        );
    }
}

enum ParitySuit: string
{
    case Hearts = 'h';
    case Spades = 's';
}
