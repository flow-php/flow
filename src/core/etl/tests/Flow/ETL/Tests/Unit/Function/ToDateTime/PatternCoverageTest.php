<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\ToDateTime;

use Flow\ETL\Function\ToDateTime\PatternCoverage;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class PatternCoverageTest extends FlowTestCase
{
    #[TestWith(['Y-m-d H:i:s'])]
    #[TestWith(['Y-m-d H'])]
    #[TestWith(['d/m/Y H:i'])]
    #[TestWith(['Y-m-d\TH:i:sP'])]
    #[TestWith(['Y z H'])]
    #[TestWith(['!Y-m-d'])]
    #[TestWith(['Y-m-d|'])]
    #[TestWith(['U'])]
    public function test_a_pattern_parsing_every_field_or_resetting_them_does_not_read_the_clock(string $pattern): void
    {
        static::assertFalse((new PatternCoverage($pattern))->fillsFromClock());
    }

    #[TestWith(['Y-m-d'])]
    #[TestWith(['H:i:s'])]
    #[TestWith(['Y-m H:i'])]
    #[TestWith(['m-d H:i'])]
    #[TestWith(['Y-m-d \H'])]
    #[TestWith(['\U Y-m-d'])]
    public function test_a_pattern_leaving_a_field_out_reads_the_clock(string $pattern): void
    {
        static::assertTrue((new PatternCoverage($pattern))->fillsFromClock());
    }

    public function test_any_finds_one_of_the_characters(): void
    {
        $coverage = new PatternCoverage('');

        static::assertTrue($coverage->any('Y-m-d', 'xm'));
        static::assertFalse($coverage->any('Y-m-d', 'HG'));
    }
}
