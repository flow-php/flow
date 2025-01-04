<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\{type_int, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\{DataProvider};

final class MetadataTest extends FlowTestCase
{
    public static function provider_test_get_as() : \Generator
    {
        yield ['test', type_string(), 'test'];
        yield [1.01, type_string(), '1.01'];
        yield [true, type_string(), 'true'];
        yield [[1, 2, 3], type_string(), '[1,2,3]'];
        yield ['1', type_int(), 1];
    }

    public function test_equal_metadata() : void
    {
        self::assertTrue(Metadata::empty()->add('array', [1, 2, 3])->isEqual(Metadata::empty()->add('array', [1, 2, 3])));
        self::assertFalse(Metadata::empty()->add('array', [1, 2, 3])->isEqual(Metadata::empty()->add('array', [2, 3])));
    }

    #[DataProvider('provider_test_get_as')]
    public function test_get_as(mixed $intput, Type $type, mixed $output) : void
    {
        self::assertEquals(
            $output,
            Metadata::empty()->add('name', $intput)->getAs('name', $type)
        );
    }

    public function test_get_as_default_value() : void
    {
        self::assertEquals(
            100,
            Metadata::empty()->getAs('name', type_int(), 100)
        );
    }

    public function test_get_non_existing_key() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('There no is key: test');

        Metadata::empty()->get('test');
    }

    public function test_merge_metadata() : void
    {
        self::assertEquals(
            Metadata::empty()->add('id', 1)->add('name', 'test'),
            Metadata::empty()->add('id', 1)->merge(Metadata::empty()->add('name', 'test'))
        );
    }

    public function test_merge_metadata_with_the_same_keys() : void
    {
        self::assertEquals(
            Metadata::empty()->add('id', 2),
            Metadata::empty()->add('id', 1)->merge(Metadata::empty()->add('id', 2))
        );
    }

    public function test_metadata_has() : void
    {
        self::assertTrue(Metadata::empty()->add('name', 'test')->has('name'));
        self::assertFalse(Metadata::empty()->has('name'));
    }

    public function test_remove_metadata_with_the_same_keys() : void
    {
        self::assertEquals(
            Metadata::empty()->add('name', 'test'),
            Metadata::empty()->add('id', 1)->add('name', 'test')->remove('id')
        );
    }
}
