<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\HashTransformer;
use PHPUnit\Framework\TestCase;

final class HashTransformerTest extends TestCase
{
    public function test_hashing_multiple_columns() : void
    {
        $transformer = new HashTransformer(['id', 'name'], 'sha256');

        $rows = $transformer->transform(new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('name', 'Johny'))
        ));

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'Johny'), Entry::string('hash', '037ffc1bf476edb863f8d20b4cfb18cc68702da50d6a06c3ac3fed933d328418'))
            ),
            $rows
        );
    }

    public function test_hashing_non_existing_column() : void
    {
        $transformer = new HashTransformer(['test'], 'sha256');

        $rows = $transformer->transform(new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('name', 'Johny'))
        ));

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'Johny'), Entry::string('hash', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'))
            ),
            $rows
        );
    }
}
