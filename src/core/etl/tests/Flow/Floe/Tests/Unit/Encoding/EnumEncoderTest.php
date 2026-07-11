<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\Floe\Encoding\EnumEncoder;
use PHPUnit\Framework\TestCase;

use function pack;
use function strlen;

final class EnumEncoderTest extends TestCase
{
    public function test_encodes_class_and_case_name(): void
    {
        $class = BackedStringEnum::class;

        static::assertSame(
            pack('V', strlen($class)) . $class . pack('V', 3) . 'one',
            (new EnumEncoder())->encode(BackedStringEnum::one),
        );
    }
}
