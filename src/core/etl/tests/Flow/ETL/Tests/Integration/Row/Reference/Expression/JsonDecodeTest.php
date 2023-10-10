<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class JsonDecodeTest extends TestCase
{
    public function test_add_json_string_into_existing_reference() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]],
                )
            )
            ->withEntry('json', lit('{"d": 4}'))
            ->withEntry('array', ref('array')->arrayMerge(ref('json')->jsonDecode()))
            ->drop('json')
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4]],
            ],
            $memory->data
        );
    }
}
