<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Unit;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{str_entry, string_entry};
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\HashIdFactory;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Tests\FlowTestCase;

final class HashIdFactoryTest extends FlowTestCase
{
    public function test_create_row() : void
    {
        $factory = new HashIdFactory('first_name', 'last_name');

        self::assertEquals(
            string_entry(
                'id',
                \hash('xxh128', 'John:Doe')
            ),
            $factory->create(
                row(str_entry('first_name', 'John'), str_entry('last_name', 'Doe'))
            )
        );
    }

    public function test_create_row_with_different_hash() : void
    {
        $factory = (new HashIdFactory('first_name', 'last_name'))->withAlgorithm(new NativePHPHash('sha1'));

        self::assertEquals(
            string_entry(
                'id',
                \sha1('John:Doe')
            ),
            $factory->create(
                row(str_entry('first_name', 'John'), str_entry('last_name', 'Doe'))
            )
        );
    }
}
