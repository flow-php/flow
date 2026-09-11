<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class JsonEncodeTest extends FlowTestCase
{
    public function test_json_encode_on_datetime(): void
    {
        $result = ref('value')
            ->jsonEncode()
            ->eval(row(['value' => new DateTimeImmutable('2021-01-01')]), flow_context());

        static::assertInstanceOf(Json::class, $result);
        static::assertSame(
            '{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}',
            $result->toString(),
        );
    }

    public function test_json_encode_on_integer(): void
    {
        static::assertSame('125', ref('value')->jsonEncode()->eval(row(['value' => 125]), flow_context()));
    }

    public function test_json_encode_on_string(): void
    {
        static::assertSame('"test"', ref('value')->jsonEncode()->eval(row(['value' => 'test']), flow_context()));
    }

    public function test_json_encode_on_valid_associative_array(): void
    {
        $result = ref('value')->jsonEncode()->eval(row(['value' => ['value' => 1]]), flow_context());

        static::assertInstanceOf(Json::class, $result);
        static::assertSame('{"value":1}', $result->toString());
    }
}
