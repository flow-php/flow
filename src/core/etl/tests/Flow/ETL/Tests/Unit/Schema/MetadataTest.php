<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

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
}
