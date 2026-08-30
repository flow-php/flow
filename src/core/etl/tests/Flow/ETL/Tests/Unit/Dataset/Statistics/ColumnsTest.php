<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use Flow\ETL\Dataset\Statistics\Columns;
use Flow\ETL\Tests\FlowTestCase;
use InvalidArgumentException;

use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\string_schema;

final class ColumnsTest extends FlowTestCase
{
    public function test_columns_statistics(): void
    {
        $columns = new Columns();

        $columns->add(integer_schema('a'), 1);
        $columns->add(integer_schema('a'), 100);
        $columns->add(integer_schema('a'), -5);
        $columns->add(string_schema('b', true), 'a');
        $columns->add(string_schema('b', true), 'some text');
        $columns->add(string_schema('b', true), null);

        static::assertCount(2, $columns->all());
        static::assertSame(3, $columns->get('a')->distinctCount());
    }

    public function test_get_non_existing_column(): void
    {
        $columns = new Columns();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "non_existing" does not exist.');

        $columns->get('non_existing');
    }
}
