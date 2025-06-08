<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{data_frame, from_array, lit, ref, to_memory};
use function Flow\Types\DSL\{type_integer, type_list};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class CallUserFuncTest extends FlowTestCase
{
    public function test_call() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['integers' => '1,2,3'],
                    ]
                )
            )
            ->withEntry(
                'integers',
                ref('integers')->call(lit('explode'), ['separator' => ','], refAlias: 'string', returnType: type_list(type_integer()))
            )
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['integers' => [1, 2, 3]],
            ],
            $memory->dump()
        );
    }
}
