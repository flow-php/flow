<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cast;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Cast\CastToDateTime;
use PHPUnit\Framework\TestCase;

final class CastToDateTest extends TestCase
{
    public function test_cast_string_to_date() : void
    {
        $this->assertEquals(
            [
                'date' => new \DateTimeImmutable('2020-01-01'),
            ],
            (new CastToDateTime(['date']))->convert(Row::create(Entry::string('date', '2020-01-01')))->toArray()
        );
    }
}
