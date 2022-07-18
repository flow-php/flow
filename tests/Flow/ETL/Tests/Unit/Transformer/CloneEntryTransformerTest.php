<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

class CloneEntryTransformerTest extends TestCase
{
    public function test_cloning_entries() : void
    {
        $rows = Transform::clone_entry('id', 'id-copy')
            ->transform(
                new Rows(
                    Row::create(new Entry\IntegerEntry('id', 1))
                ),
                new FlowContext(Config::default())
            );

        $this->assertSame(
            [
                ['id' => 1, 'id-copy' => 1],
            ],
            $rows->toArray()
        );
    }
}
