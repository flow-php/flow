<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit\Pagination;

use Flow\ETL\Adapter\Doctrine\Pagination\Key;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;

final class KeySetTest extends FlowTestCase
{
    public function test_keys_are_reversed_into_a_list(): void
    {
        static::assertSame(
            ['created_at', 'id'],
            array_map(
                static fn(Key $key): string => $key->column,
                pagination_key_set(pagination_key_asc('id'), pagination_key_asc('created_at'))->keys,
            ),
        );
    }

    public function test_requires_at_least_one_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('KeySet requires at least one key');

        pagination_key_set();
    }
}
