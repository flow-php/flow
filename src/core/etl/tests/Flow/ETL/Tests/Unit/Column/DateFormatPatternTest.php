<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use Flow\ETL\Column\DateFormatPattern;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DateFormatPatternTest extends TestCase
{
    /**
     * @param list<string> $letters
     */
    #[TestWith(['Y-m-d\TH:i:s.uP', ['Y', 'm', 'd', 'H', 'i', 's', 'u', 'P']])]
    #[TestWith(['\Y\m', []])]
    #[TestWith(['', []])]
    #[TestWith(['1 2-3', []])]
    #[TestWith(['\\\\T', ['T']])]
    #[TestWith(['Y\\', ['Y']])]
    public function test_letters(string $format, array $letters): void
    {
        static::assertSame($letters, (new DateFormatPattern($format))->letters());
    }

    /**
     * @param list<array{string, string}> $segments
     */
    #[TestWith([
        'Y-m-d\TH:i:s.uP',
        [
            ['Y-m-d\TH:i:s.', 'u'],
            ['P',             ''],
        ],
    ])]
    #[TestWith(['Y-m-d', [['Y-m-d', '']]])]
    #[TestWith(['', []])]
    #[TestWith(['u', [['', 'u']]])]
    #[TestWith(['uv', [['', 'u'], ['', 'v']]])]
    #[TestWith(['H.v', [['H.', 'v']]])]
    #[TestWith(['\u \v u', [['\u \v ', 'u']]])]
    #[TestWith(['Y\\', [['Y\\', '']]])]
    public function test_segments(string $format, array $segments): void
    {
        static::assertSame($segments, (new DateFormatPattern($format))->segments());
    }
}
