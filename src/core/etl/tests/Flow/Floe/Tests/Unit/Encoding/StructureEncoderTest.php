<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\DynamicEncoder;
use Flow\Floe\Encoding\Int64Encoder;
use Flow\Floe\Encoding\StringEncoder;
use Flow\Floe\Encoding\StructureEncoder;
use Flow\Floe\Format;
use PHPUnit\Framework\TestCase;

use function chr;
use function pack;

final class StructureEncoderTest extends TestCase
{
    public function test_encodes_present_null_and_absent_element_flags(): void
    {
        $encoder = new StructureEncoder(
            ['id' => new Int64Encoder(), 'name' => new StringEncoder(), 'missing' => new Int64Encoder()],
            false,
            new DynamicEncoder(),
        );

        static::assertSame(
            Format::VALUE_PRESENT_BYTE . pack('P', 1) . Format::VALUE_NULL_BYTE . Format::VALUE_ABSENT_BYTE,
            $encoder->encode(['id' => 1, 'name' => null]),
        );
    }

    public function test_encodes_extras_dynamically_when_allowed(): void
    {
        $encoder = new StructureEncoder(['id' => new Int64Encoder()], true, new DynamicEncoder());

        static::assertSame(
            Format::VALUE_PRESENT_BYTE . pack('P', 1) . pack('V', 1) . pack('V', 5) . 'extra' . chr(Format::TAG_INTEGER)
                . pack('P', 2),
            $encoder->encode(['id' => 1, 'extra' => 2]),
        );
    }
}
