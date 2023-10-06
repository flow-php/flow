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

final class AddJsonTest extends TestCase
{
    public function test_add_json_into_existing_reference() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]],
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->drop('row')
            ->withEntry('array', ref('array')->arrayMerge(lit(['d' => 4])))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4]],
            ],
            $memory->data
        );
    }

    public function test_add_json_string_into_existing_reference() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]],
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->drop('row')
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

    public function test_adding_json_as_object_from_string_entry() : void
    {
        (new Flow())
            ->read(
                From::array([['id' => 1]])
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->drop('row')
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
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->drop('row')
            ->withEntry('json', lit('[{"id":1},{"id":2}]'))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                [
                    'id' => 1,
                    'json' => '[{"id":1},{"id":2}]',
                ],
            ],
            $memory->data
        );
    }
}
