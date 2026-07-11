<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use DateTimeImmutable;
use Flow\Floe\Encoding\DynamicEncoder;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Tests\Mother\DynamicDecoderMother;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

final class DynamicDecoderTest extends TestCase
{
    public function test_round_trip_of_tagged_values(): void
    {
        $encoder = new DynamicEncoder();
        $decoder = DynamicDecoderMother::create();

        $values = [
            'null' => null,
            'int' => 42,
            'float' => 3.5,
            'bool' => false,
            'string' => 'text',
            'array' => ['a' => 1, 0 => 'zero', 'nested' => ['x' => [1, 2]]],
        ];

        foreach ($values as $label => $value) {
            $position = 0;

            static::assertSame($value, $decoder->decode($encoder->encode($value), $position), $label);
        }
    }

    public function test_round_trip_of_tagged_objects(): void
    {
        $encoder = new DynamicEncoder();
        $decoder = DynamicDecoderMother::create();

        $position = 0;
        $datetime = new DateTimeImmutable('2025-01-01 00:00:00.123456 UTC');
        static::assertEquals($datetime, $decoder->decode($encoder->encode($datetime), $position));

        $position = 0;
        $uuid = new Uuid('0196aecb-b568-7e57-a381-8ec8d3e4a531');
        // @mago-ignore analysis:mixed-assignment
        $decodedUuid = $decoder->decode($encoder->encode($uuid), $position);
        static::assertInstanceOf(Uuid::class, $decodedUuid);
        static::assertSame($uuid->toString(), $decodedUuid->toString());

        $position = 0;
        $json = new Json('{"a":true}');
        // @mago-ignore analysis:mixed-assignment
        $decodedJson = $decoder->decode($encoder->encode($json), $position);
        static::assertInstanceOf(Json::class, $decodedJson);
        static::assertSame($json->toString(), $decodedJson->toString());
    }

    public function test_unknown_tag_throws(): void
    {
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown dynamic value tag');

        DynamicDecoderMother::create()->decode("\xEE", $position);
    }
}
