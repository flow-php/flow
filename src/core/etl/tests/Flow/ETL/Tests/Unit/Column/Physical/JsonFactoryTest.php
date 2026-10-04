<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\JsonFactory;
use Flow\Types\Value\Json;
use PHPUnit\Framework\TestCase;

final class JsonFactoryTest extends TestCase
{
    public function test_builds_json_equal_to_the_validating_constructor(): void
    {
        static::assertEquals(Json::fromString('{"a":1}'), (new JsonFactory())->create('{"a":1}'));
        static::assertEquals(Json::fromString('[1,2]'), (new JsonFactory())->create('[1,2]'));
        static::assertTrue(
            (new JsonFactory())
                ->create('{}')
                ->isObject(),
        );
        static::assertFalse(
            (new JsonFactory())
                ->create('[]')
                ->isObject(),
        );
    }
}
