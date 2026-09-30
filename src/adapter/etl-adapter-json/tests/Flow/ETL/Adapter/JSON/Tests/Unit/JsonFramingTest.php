<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use Flow\ETL\Adapter\JSON\JsonFraming;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class JsonFramingTest extends TestCase
{
    #[TestWith([JsonFraming::LINES, '', "\n", "\n", ''])]
    #[TestWith([JsonFraming::ARRAY, '[', ',', ']', '[]'])]
    #[TestWith([JsonFraming::ARRAY_LINES, "[\n", ",\n", "\n]", "[\n\n]"])]
    public function test_framing(
        JsonFraming $framing,
        string $opening,
        string $separator,
        string $closing,
        string $empty,
    ): void {
        static::assertSame([$opening, $separator, $closing, $empty], [
            $framing->opening(),
            $framing->separator(),
            $framing->closing(),
            $framing->empty(),
        ]);
    }
}
