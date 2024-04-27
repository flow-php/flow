<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{array_entry, int_entry, row, rows, string_entry};
use Flow\ETL\Transformer\DropEntriesTransformer;
use Flow\ETL\{Config, FlowContext};
use PHPUnit\Framework\TestCase;

final class DropEntriesTransformerTest extends TestCase
{
    public function test_dropping_entries() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1),
                string_entry('name', 'Row Name'),
                array_entry('array', ['test'])
            )
        );

        $transformer = new DropEntriesTransformer('id', 'array');
        self::assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_removing_not_existing_entries() : void
    {
        $rows = rows(
            row(
                int_entry('id', 1),
                string_entry('name', 'Row Name'),
                array_entry('array', ['test'])
            )
        );

        $transformer = new DropEntriesTransformer('not_existing');
        self::assertSame(
            [
                ['id' => 1, 'name' => 'Row Name', 'array' => ['test']],
            ],
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }
}
