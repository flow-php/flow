<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Join\Comparison;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Row;

final class AnyTest extends TestCase
{
    public function test_failure() : void
    {
        $comparison1 = $this->createStub(Comparison::class);
        $comparison1
            ->method('compare')
            ->willReturn(false);

        $comparison2 = $this->createStub(Comparison::class);
        $comparison2
            ->method('compare')
            ->willReturn(false);

        $this->assertFalse(
            (new Any($comparison1, $comparison2))
                ->compare(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2)),
                )
        );
    }

    public function test_success() : void
    {
        $comparison1 = $this->createStub(Comparison::class);
        $comparison1
            ->method('compare')
            ->willReturn(true);

        $comparison2 = $this->createStub(Comparison::class);
        $comparison2
            ->method('compare')
            ->willReturn(false);

        $this->assertTrue(
            (new Any($comparison1, $comparison2))
                ->compare(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2)),
                )
        );
    }
}
