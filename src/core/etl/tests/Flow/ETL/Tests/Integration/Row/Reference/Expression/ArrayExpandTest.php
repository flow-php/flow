<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Row\Reference\Expression\ArrayExpand\ArrayExpand;
use PHPUnit\Framework\TestCase;

final class ArrayExpandTest extends TestCase
{
    public function test_expand_both() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->withEntry('expanded', array_expand(ref('array'), ArrayExpand::BOTH))
            ->drop('row', 'array')
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'expanded' => ['a' => 1]],
                ['id' => 1, 'expanded' => ['b' => 2]],
                ['id' => 1, 'expanded' => ['c' => 3]],
            ],
            $memory->data
        );
    }

    public function test_expand_keys() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->withEntry('expanded', array_expand(ref('array'), ArrayExpand::KEYS))
            ->drop('row', 'array')
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'expanded' => 'a'],
                ['id' => 1, 'expanded' => 'b'],
                ['id' => 1, 'expanded' => 'c'],
            ],
            $memory->data
        );
    }

    public function test_expand_values() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->withEntry('expanded', array_expand(ref('array')))
            ->drop('row', 'array')
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'expanded' => 1],
                ['id' => 1, 'expanded' => 2],
                ['id' => 1, 'expanded' => 3],
            ],
            $memory->data
        );
    }
}
