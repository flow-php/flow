<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class CallUserFuncTest extends FlowTestCase
{
    public function test_call(): void
    {
        data_frame()
            ->read(from_array([
                ['integers' => '1,2,3'],
            ]))
            ->withEntry('integers', ref('integers')->call(
                lit('explode'),
                ['separator' => ','],
                refAlias: 'string',
                returnType: type_list(type_integer()),
            ))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['integers' => [1, 2, 3]],
            ],
            $memory->dump(),
        );
    }
}
