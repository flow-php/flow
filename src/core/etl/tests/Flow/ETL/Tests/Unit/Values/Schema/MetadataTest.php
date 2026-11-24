<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
use function Flow\ETL\DSL\schema_metadata;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class MetadataTest extends FlowTestCase
{
    public static function provider_test_get_as(): Generator
    {
        yield ['test', type_string(), 'test'];
        yield [1.01, type_string(), '1.01'];
        yield [true, type_string(), 'true'];
        yield [[1, 2, 3], type_string(), '[1,2,3]'];
        yield ['1', type_integer(), 1];
    }

    public function test_equal_metadata(): void
    {
        static::assertTrue(Metadata::empty()->add('array', [1, 2, 3])->isEqual(Metadata::empty()->add('array', [
            1,
            2,
            3,
        ])));
        static::assertFalse(Metadata::empty()->add('array', [1, 2, 3])->isEqual(Metadata::empty()->add('array', [
            2,
            3,
        ])));
    }

    public function test_is_equal_empty(): void
    {
        static::assertTrue(Metadata::empty()->isEqual(Metadata::empty()));
    }

    public function test_is_equal_identical(): void
    {
        static::assertTrue(Metadata::empty()->add('id', 1)->isEqual(Metadata::empty()->add('id', 1)));
    }

    public function test_is_equal_differing_key(): void
    {
        static::assertFalse(Metadata::empty()->add('a', 1)->isEqual(Metadata::empty()->add('b', 1)));
    }

    public function test_is_equal_differing_value(): void
    {
        static::assertFalse(Metadata::empty()->add('a', 1)->isEqual(Metadata::empty()->add('a', 2)));
    }

    /**
     * @param array<mixed>|bool|float|int|string $intput
     * @param Type<mixed> $type
     * @param mixed $output
     */
    #[DataProvider('provider_test_get_as')]
    public function test_get_as(int|string|float|bool|array $intput, Type $type, mixed $output): void
    {
        static::assertEquals($output, Metadata::empty()->add('name', $intput)->getAs('name', $type));
    }

    public function test_get_as_default_value(): void
    {
        static::assertEquals(100, Metadata::empty()->getAs('name', type_integer(), 100));
    }

    public function test_get_non_existing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('There no is key: test');

        Metadata::empty()->get('test');
    }

    public function test_merge_metadata(): void
    {
        static::assertEquals(
            Metadata::empty()->add('id', 1)->add('name', 'test'),
            Metadata::empty()->add('id', 1)->merge(Metadata::empty()->add('name', 'test')),
        );
    }

    public function test_merge_metadata_with_the_same_keys(): void
    {
        static::assertEquals(
            Metadata::empty()->add('id', 2),
            Metadata::empty()->add('id', 1)->merge(Metadata::empty()->add('id', 2)),
        );
    }

    public function test_metadata_has(): void
    {
        static::assertTrue(Metadata::empty()->add('name', 'test')->has('name'));
        static::assertFalse(Metadata::empty()->has('name'));
    }

    public function test_remove_metadata_with_the_same_keys(): void
    {
        static::assertEquals(
            Metadata::empty()->add('name', 'test'),
            Metadata::empty()->add('id', 1)->add('name', 'test')->remove('id'),
        );
    }

    public function test_use_object_in_metadata_array(): void
    {
        $this->expectExceptionMessage('Metadata value must be a scalar or an array of scalars');

        Metadata::empty()->add('object', [new DateTimeImmutable()]);
    }

    public function test_normalize_sorts_map_keys_at_every_depth(): void
    {
        static::assertSame(
            ['a' => ['x' => 2, 'y' => 1], 'b' => 1],
            schema_metadata(['b' => 1, 'a' => ['y' => 1, 'x' => 2]])->normalize(),
        );
    }

    public function test_map_keys_are_sorted_as_bytes(): void
    {
        static::assertSame(
            [10, 9, 'B', 'b'],
            array_keys(
                Metadata::empty()
                    ->add('b', 1)
                    ->add('B', 1)
                    ->add('9', 1)
                    ->add('10', 1)
                    ->normalize(),
            ),
        );
    }

    public function test_list_keeps_its_order(): void
    {
        static::assertSame(['b', 'a'], schema_metadata(['tags' => ['b', 'a']])->get('tags'));
    }

    public function test_is_equal_respects_list_order(): void
    {
        static::assertFalse(schema_metadata(['tags' => ['a', 'b']])->isEqual(schema_metadata(['tags' => ['b', 'a']])));
    }

    public function test_add_keeps_a_numeric_string_key(): void
    {
        static::assertSame('x', Metadata::with('5', 'x')->add('k', 'v')->get('5'));
    }

    public function test_merge_keeps_numeric_string_keys(): void
    {
        static::assertSame(
            [5 => 'x', 7 => 'y'],
            Metadata::with('5', 'x')->merge(Metadata::with('7', 'y'))->normalize(),
        );
    }

    public function test_add_produces_canonical_order(): void
    {
        static::assertSame(['a' => 2, 'b' => 1], Metadata::empty()->add('b', 1)->add('a', 2)->normalize());
    }
}
