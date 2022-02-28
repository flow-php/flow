<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class RemoveEntriesTransformerTest extends TestCase
{
    public function test_removing_entries() : void
    {
        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Row Name'),
                new Row\Entry\ArrayEntry('array', ['test'])
            )
        );

        $transformer = Transform::remove('id', 'array');
        $this->assertSame(
            [
                ['name' => 'Row Name'],
            ],
            $transformer->transform($rows)->toArray()
        );
    }

    public function test_removing_not_existing_entries() : void
    {
        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Row Name'),
                new Row\Entry\ArrayEntry('array', ['test'])
            )
        );

        $transformer = Transform::remove('not_existing');
        $this->assertSame(
            [
                ['id' => 1, 'name' => 'Row Name', 'array' => ['test']],
            ],
            $transformer->transform($rows)->toArray()
        );
    }
}
