<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use function Flow\ETL\DSL\{integer_entry, string_entry};
use Flow\ETL\Dataset\Statistics\Columns;
use Flow\ETL\Tests\FlowTestCase;

final class ColumnsTest extends FlowTestCase
{
    public function test_columns_statistics() : void
    {
        $columns = new Columns();

        $columns->add(integer_entry('a', 1));
        $columns->add(integer_entry('a', 100));
        $columns->add(integer_entry('a', -5));
        $columns->add(string_entry('b', 'a'));
        $columns->add(string_entry('b', 'some text'));
        $columns->add(string_entry('b', null));

        self::assertCount(2, $columns->all());
        self::assertSame(3, $columns->get('a')->distinctCount());
    }

    public function test_get_non_existing_column() : void
    {
        $columns = new Columns();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "non_existing" does not exist.');

        $columns->get('non_existing');
    }
}
