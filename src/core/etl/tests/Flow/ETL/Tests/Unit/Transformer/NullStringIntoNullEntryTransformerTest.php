<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

/**
 * @psalm-immutable
 */
final class NullStringIntoNullEntryTransformerTest extends TestCase
{
    public function test_transforms_null_string_entries_into_null_entries() : void
    {
        $transformer = Transform::to_null_from_null_string('description', 'recommendation');

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\BooleanEntry('active', false),
                new Row\Entry\StringEntry('name', 'NULL'),
                new Row\Entry\StringEntry('description', 'NULL'),
                new Row\Entry\StringEntry('recommendation', 'null')
            )
        ), new FlowContext(Config::default()));

        $this->assertSame(
            [[
                'id' => 1,
                'active' => false,
                'name' => 'NULL',
                'description' => null,
                'recommendation' => null,
            ]],
            $rows->toArray()
        );
    }
}
