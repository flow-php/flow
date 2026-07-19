<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Options;
use Flow\Floe\Tests\Double\CodecStub;
use PHPUnit\Framework\TestCase;

final class OptionsTest extends TestCase
{
    public function test_default_values(): void
    {
        $options = new Options();

        static::assertTrue($options->validateData);
        static::assertSame(65_536, $options->bufferSize);
        static::assertInstanceOf(NoopCodec::class, $options->codec);
    }

    public function test_default_factory_matches_the_constructor_defaults(): void
    {
        $options = Options::default();

        static::assertTrue($options->validateData);
        static::assertSame(65_536, $options->bufferSize);
        static::assertInstanceOf(NoopCodec::class, $options->codec);
    }

    public function test_with_validate_data_returns_a_new_instance_and_keeps_the_rest(): void
    {
        $original = new Options();
        $modified = $original->withValidateData(false);

        static::assertNotSame($original, $modified);
        static::assertTrue($original->validateData);
        static::assertFalse($modified->validateData);
        static::assertSame($original->bufferSize, $modified->bufferSize);
        static::assertSame($original->codec, $modified->codec);
    }

    public function test_with_buffer_size_returns_a_new_instance_and_keeps_the_rest(): void
    {
        $original = new Options();
        $modified = $original->withBufferSize(4_096);

        static::assertNotSame($original, $modified);
        static::assertSame(65_536, $original->bufferSize);
        static::assertSame(4_096, $modified->bufferSize);
        static::assertSame($original->validateData, $modified->validateData);
        static::assertSame($original->codec, $modified->codec);
    }

    public function test_with_codec_returns_a_new_instance_and_keeps_the_rest(): void
    {
        $original = new Options();
        $codec = new CodecStub(0x00);
        $modified = $original->withCodec($codec);

        static::assertNotSame($original, $modified);
        static::assertInstanceOf(NoopCodec::class, $original->codec);
        static::assertSame($codec, $modified->codec);
        static::assertSame($original->validateData, $modified->validateData);
        static::assertSame($original->bufferSize, $modified->bufferSize);
    }
}
