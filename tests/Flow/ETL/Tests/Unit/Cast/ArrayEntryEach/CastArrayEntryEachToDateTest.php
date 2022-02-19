<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cast\ArrayEntryEach;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Transformer\Cast\ArrayEntryEach\CastArrayEntryEachToDateTime;
use PHPUnit\Framework\TestCase;

final class CastArrayEntryEachToDateTest extends TestCase
{
    public function test_casting_each_element_of_array_entry_to_date() : void
    {
        $arrayEntry = new ArrayEntry(
            'dates',
            [
                '2020-01-01',
                '2020-01-02',
                '2020-01-03',
            ]
        );

        $caster = new CastArrayEntryEachToDateTime('dates');

        $this->assertEquals(
            [
                new \DateTimeImmutable('2020-01-01'),
                new \DateTimeImmutable('2020-01-02'),
                new \DateTimeImmutable('2020-01-03'),
            ],
            $caster->cast(Row::create($arrayEntry))->valueOf('dates')
        );
    }
}
