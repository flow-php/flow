<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\type_json;
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Caster\JsonCastingHandler;
use PHPUnit\Framework\TestCase;

final class JsonCastingHandlerTest extends TestCase
{
    public function test_casting_array_to_json() : void
    {
        $this->assertSame(
            '{"items":{"item":1}}',
            (new JsonCastingHandler())->value(['items' => ['item' => 1]], type_json(), Caster::default())
        );
    }

    public function test_casting_datetime_to_json() : void
    {
        $this->assertSame(
            '{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}',
            (new JsonCastingHandler())->value(new \DateTimeImmutable('2021-01-01 00:00:00 UTC'), type_json(), Caster::default())
        );
    }

    public function test_casting_integer_to_json() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "integer" into "json" type');

        (new JsonCastingHandler())->value(1, type_json(), Caster::default());
    }

    public function test_casting_json_string_to_json() : void
    {
        $this->assertSame(
            '{"items":{"item":1}}',
            (new JsonCastingHandler())->value('{"items":{"item":1}}', type_json(), Caster::default())
        );
    }

    public function test_casting_non_json_string_to_json() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "string" into "json" type');

        (new JsonCastingHandler())->value('string', type_json(), Caster::default());
    }
}
