<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Describe;

use Flow\ETL\Foundation\Describe\Json;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use JsonException;

final class JsonTest extends FlowTestCase
{
    public function test_a_float_keeps_its_fraction(): void
    {
        static::assertSame('{"a":1.0}', Json::encode(['a' => 1.0]));
        static::assertSame(['a' => 1.0], Json::decode('{"a":1.0}'));
    }

    public function test_slashes_are_not_escaped(): void
    {
        static::assertSame('{"path":"file:///tmp/a.csv"}', Json::encode(['path' => 'file:///tmp/a.csv']));
    }

    public function test_invalid_utf8_throws(): void
    {
        $this->expectException(JsonException::class);

        Json::encode(['a' => "\xff"]);
    }

    public function test_a_scalar_document_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected a JSON array or object');

        Json::decode('1');
    }

    public function test_malformed_json_throws(): void
    {
        $this->expectException(JsonException::class);

        Json::decode('{');
    }
}
