<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel;

use Flow\Parquet\Dremel\DremelShredder;
use Flow\Parquet\Dremel\Validator\DisabledValidator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class DremelShredderTest extends TestCase
{
    public function test_complex_schema() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('id')->makeRequired(),
            FlatColumn::string('name'),
            FlatColumn::double('score'),
            NestedColumn::structure('address', [
                FlatColumn::string('city'),
                FlatColumn::string('zip'),
            ]),
            NestedColumn::list('tags', ListElement::string()),
            NestedColumn::map('metadata', MapKey::string(), MapValue::int32()),
        );

        $rows = [
            ['id' => 1, 'name' => 'alice', 'score' => 9.5, 'address' => ['city' => 'NYC', 'zip' => '10001'], 'tags' => ['a', 'b'], 'metadata' => ['k1' => 1]],
            ['id' => 2, 'name' => null, 'score' => null, 'address' => null, 'tags' => null, 'metadata' => null],
            ['id' => 3, 'name' => 'charlie', 'score' => 7.0, 'address' => ['city' => 'LA', 'zip' => null], 'tags' => [], 'metadata' => []],
        ];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['id', 'name', 'score', 'address.city', 'address.zip', 'tags.list.element', 'metadata.key_value.key', 'metadata.key_value.value'], \array_keys($result));

        self::assertSame([1, 2, 3], $result['id']->values());
        self::assertSame(['alice', 'charlie'], $result['name']->values());
        self::assertSame([9.5, 7.0], $result['score']->values());
        self::assertSame(['NYC', 'LA'], $result['address.city']->values());
        self::assertSame(['10001'], $result['address.zip']->values());
        self::assertSame(['a', 'b'], $result['tags.list.element']->values());
        self::assertSame(['k1'], $result['metadata.key_value.key']->values());
        self::assertSame([1], $result['metadata.key_value.value']->values());
    }

    public function test_flat_optional_column() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $rows = [['id' => 1], ['id' => null], ['id' => 3]];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['id'], \array_keys($result));
        self::assertSame([1, 3], $result['id']->values());
        self::assertSame([0, 0, 0], $result['id']->repetitionLevels());
        self::assertSame([1, 0, 1], $result['id']->definitionLevels());
    }

    public function test_flat_optional_column_all_nulls() : void
    {
        $schema = Schema::with(FlatColumn::int32('id'));
        $rows = [['id' => null], ['id' => null], ['id' => null]];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['id'], \array_keys($result));
        self::assertSame([], $result['id']->values());
        self::assertSame([0, 0, 0], $result['id']->repetitionLevels());
        self::assertSame([0, 0, 0], $result['id']->definitionLevels());
    }

    public function test_flat_required_column() : void
    {
        $schema = Schema::with(FlatColumn::int32('id')->makeRequired());
        $rows = [['id' => 1], ['id' => 2], ['id' => 3]];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['id'], \array_keys($result));
        self::assertSame([1, 2, 3], $result['id']->values());
        self::assertSame([0, 0, 0], $result['id']->repetitionLevels());
        self::assertSame([0, 0, 0], $result['id']->definitionLevels());
    }

    public function test_list_of_flat_values() : void
    {
        $schema = Schema::with(NestedColumn::list('tags', ListElement::string()));
        $rows = [
            ['tags' => null],
            ['tags' => []],
            ['tags' => ['a', 'b', 'c']],
            ['tags' => ['d']],
        ];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['tags.list.element'], \array_keys($result));
        self::assertSame(['a', 'b', 'c', 'd'], $result['tags.list.element']->values());
        self::assertSame([0, 0, 0, 1, 1, 0], $result['tags.list.element']->repetitionLevels());
        self::assertSame([0, 1, 3, 3, 3, 3], $result['tags.list.element']->definitionLevels());
    }

    public function test_list_of_structs() : void
    {
        $schema = Schema::with(
            NestedColumn::list('items', ListElement::structure([
                FlatColumn::int32('id'),
                FlatColumn::string('name'),
            ]))
        );
        $rows = [
            ['items' => null],
            ['items' => []],
            ['items' => [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']]],
            ['items' => [['id' => 3, 'name' => 'c']]],
        ];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['items.list.element.id', 'items.list.element.name'], \array_keys($result));
        self::assertSame([1, 2, 3], $result['items.list.element.id']->values());
        self::assertSame(['a', 'b', 'c'], $result['items.list.element.name']->values());
    }

    public function test_map_of_flat_values() : void
    {
        $schema = Schema::with(NestedColumn::map('props', MapKey::string(), MapValue::int32()));
        $rows = [
            ['props' => null],
            ['props' => []],
            ['props' => ['a' => 1, 'b' => 2]],
            ['props' => ['c' => 3]],
        ];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['props.key_value.key', 'props.key_value.value'], \array_keys($result));
        self::assertSame(['a', 'b', 'c'], $result['props.key_value.key']->values());
        self::assertSame([1, 2, 3], $result['props.key_value.value']->values());
    }

    public function test_map_with_struct_values() : void
    {
        $schema = Schema::with(
            NestedColumn::map('data', MapKey::string(), MapValue::structure([
                FlatColumn::int32('x'),
                FlatColumn::string('y'),
            ]))
        );
        $rows = [
            ['data' => null],
            ['data' => ['k1' => ['x' => 1, 'y' => 'a']]],
            ['data' => ['k2' => ['x' => 2, 'y' => 'b'], 'k3' => ['x' => 3, 'y' => 'c']]],
        ];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['data.key_value.key', 'data.key_value.value.x', 'data.key_value.value.y'], \array_keys($result));
        self::assertSame(['k1', 'k2', 'k3'], $result['data.key_value.key']->values());
        self::assertSame([1, 2, 3], $result['data.key_value.value.x']->values());
        self::assertSame(['a', 'b', 'c'], $result['data.key_value.value.y']->values());
    }

    public function test_multiple_flat_columns() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('id'),
            FlatColumn::string('name'),
            FlatColumn::boolean('active'),
        );
        $rows = [
            ['id' => 1, 'name' => 'alice', 'active' => true],
            ['id' => null, 'name' => null, 'active' => false],
            ['id' => 3, 'name' => 'charlie', 'active' => null],
        ];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['id', 'name', 'active'], \array_keys($result));
        self::assertSame([1, 3], $result['id']->values());
        self::assertSame(['alice', 'charlie'], $result['name']->values());
        self::assertSame([true, false], $result['active']->values());
    }

    public function test_nested_list_of_lists() : void
    {
        $schema = Schema::with(
            NestedColumn::list('matrix', ListElement::list(ListElement::int32()))
        );
        $rows = [
            ['matrix' => null],
            ['matrix' => []],
            ['matrix' => [[1, 2], [3]]],
            ['matrix' => [[4]]],
        ];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['matrix.list.element.list.element'], \array_keys($result));
        self::assertSame([1, 2, 3, 4], $result['matrix.list.element.list.element']->values());
    }

    public function test_struct_with_flat_children() : void
    {
        $schema = Schema::with(
            NestedColumn::structure('s', [
                FlatColumn::int32('a'),
                FlatColumn::string('b'),
            ])
        );
        $rows = [
            ['s' => null],
            ['s' => ['a' => 1, 'b' => 'x']],
            ['s' => ['a' => null, 'b' => 'y']],
        ];

        $shredder = new DremelShredder(new DisabledValidator(), DataConverter::initialize(Options::default()));
        $result = $shredder->shred($schema, $rows);

        self::assertSame(['s.a', 's.b'], \array_keys($result));
        self::assertSame([1], $result['s.a']->values());
        self::assertSame(['x', 'y'], $result['s.b']->values());
    }
}
