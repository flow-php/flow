<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\CloneEntryTransformer;
use PHPUnit\Framework\TestCase;

class CloneEntryTransformerTest extends TestCase
{
    public function test_cloning_entries() : void
    {
        $rows = (new CloneEntryTransformer('id', 'id-copy'))
            ->transform(
                new Rows(
                    Row::create(new Entry\IntegerEntry('id', 1))
                )
            );

        $this->assertSame(
            [
                ['id' => 1, 'id-copy' => 1],
            ],
            $rows->toArray()
        );
    }
}
