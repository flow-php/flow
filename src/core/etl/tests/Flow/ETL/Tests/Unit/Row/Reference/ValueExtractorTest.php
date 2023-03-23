<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\ValueExtractor;
use PHPUnit\Framework\TestCase;

final class ValueExtractorTest extends TestCase
{
    public function test_extracting_default_value_row() : void
    {
        $row = Row::create(Entry::integer('int', 100));

        $this->assertNull((new ValueExtractor())->value($row, ref('not_exists')));
        $this->assertSame(100, (new ValueExtractor())->value($row, ref('not_exists'), 100));
    }

    public function test_extracting_value_from_reference_with_literal_expression() : void
    {
        $row = Row::create(Entry::integer('int', 100));

        $this->assertSame(500, (new ValueExtractor())->value($row, lit(500)));
    }

    public function test_extracting_value_from_reference_with_literal_expression_when_entry_exists() : void
    {
        $row = Row::create(Entry::integer('int', 100));

        $this->assertSame(500, (new ValueExtractor())->value($row, ref('int')->literal(500)));
    }

    public function test_extracting_value_row() : void
    {
        $row = Row::create(Entry::integer('int', 100));

        $this->assertSame(100, (new ValueExtractor())->value($row, ref('int')));
    }
}
