<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\JsonPhysical;
use Flow\Types\Value\Json;
use PHPUnit\Framework\TestCase;

final class JsonPhysicalTest extends TestCase
{
    public function test_the_json_text(): void
    {
        $physical = new JsonPhysical();

        static::assertSame('{"a":1}', $physical->toPhysical(Json::fromString('{"a":1}')));
        static::assertEquals(Json::fromString('{"a":1}'), $physical->fromPhysical('{"a":1}'));
        static::assertEquals([Json::fromString('[1]'), null], $physical->fromPhysicalAll(['[1]', null]));
    }
}
