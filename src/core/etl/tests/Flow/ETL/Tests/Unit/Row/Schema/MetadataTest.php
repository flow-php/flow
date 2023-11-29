<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Schema\Metadata;
use PHPUnit\Framework\TestCase;

final class MetadataTest extends TestCase
{
    public function test_equal_metadata() : void
    {
        $this->assertTrue(Metadata::empty()->add('array', [1, 2, 3])->isEqual(Metadata::empty()->add('array', [1, 2, 3])));
        $this->assertFalse(Metadata::empty()->add('array', [1, 2, 3])->isEqual(Metadata::empty()->add('array', [2, 3])));
    }

    public function test_get_non_existing_key() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('There no is key: test');

        Metadata::empty()->get('test');
    }

    public function test_merge_metadata() : void
    {
        $this->assertEquals(
            Metadata::empty()->add('id', 1)->add('name', 'test'),
            Metadata::empty()->add('id', 1)->merge(Metadata::empty()->add('name', 'test'))
        );
    }

    public function test_merge_metadata_with_the_same_keys() : void
    {
        $this->assertEquals(
            Metadata::empty()->add('id', 2),
            Metadata::empty()->add('id', 1)->merge(Metadata::empty()->add('id', 2))
        );
    }

    public function test_merge_object_metadata() : void
    {
        $this->assertEquals(
            Metadata::empty()->add('type', type_int()),
            Metadata::empty()->add('type', type_string())->merge(Metadata::empty()->add('type', type_int()))
        );
    }

    public function test_remove_metadata_with_the_same_keys() : void
    {
        $this->assertEquals(
            Metadata::empty()->add('name', 'test'),
            Metadata::empty()->add('id', 1)->add('name', 'test')->remove('id')
        );
    }
}
