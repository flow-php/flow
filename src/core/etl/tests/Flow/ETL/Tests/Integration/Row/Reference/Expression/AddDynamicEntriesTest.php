<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class AddDynamicEntriesTest extends TestCase
{
    public function test_adding_new_entries() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('updated_at', lit(new \DateTimeImmutable('2020-01-01 00:00:00 UTC')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertEquals(
            [
                ['id' => 1, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
                ['id' => 2, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
            ],
            $memory->data
        );
    }
}
