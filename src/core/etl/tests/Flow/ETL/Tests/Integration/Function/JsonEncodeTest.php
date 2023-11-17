<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class JsonEncodeTest extends TestCase
{
    public function test_adding_json_as_object_from_string_entry() : void
    {
        (new Flow())
            ->read(
                From::array([['id' => 1]])
            )
            ->withEntry('json', lit(['id' => 1, 'name' => 'test']))
            ->withEntry('json', ref('json')->jsonEncode(\JSON_FORCE_OBJECT))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                [
                    'id' => 1,
                    'json' => '{"id":1,"name":"test"}',
                ],
            ],
            $memory->data
        );
    }

    public function test_adding_json_from_string_entry() : void
    {
        (new Flow())
            ->read(
                From::array([['id' => 1]])
            )
            ->withEntry('json', lit([1, 2, 3]))
            ->withEntry('json', ref('json')->jsonEncode())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                [
                    'id' => 1,
                    'json' => '[1,2,3]',
                ],
            ],
            $memory->data
        );
    }
}
