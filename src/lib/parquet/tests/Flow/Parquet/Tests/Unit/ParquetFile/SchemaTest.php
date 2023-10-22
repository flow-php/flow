<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile;

use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

final class SchemaTest extends TestCase
{
    public function test_calculating_repetition_and_definition_for_nested_fields() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('int'),
            NestedColumn::struct(
                'nested',
                [
                    FlatColumn::int32('int'),
                    FlatColumn::string('strings'),
                    NestedColumn::struct(
                        'nested',
                        [
                            FlatColumn::boolean('bool'),
                        ]
                    ),
                    NestedColumn::list('list_of_ints', ListElement::int32()),
                ]
            ),
        );

        $this->assertSame(2, $schema->get('int')->maxDefinitionsLevel());
        $this->assertSame(0, $schema->get('int')->maxRepetitionsLevel());
        $this->assertSame(3, $schema->get('nested.int')->maxDefinitionsLevel());
        $this->assertSame(0, $schema->get('nested.int')->maxRepetitionsLevel());
        $this->assertSame(4, $schema->get('nested.nested.bool')->maxDefinitionsLevel());
        $this->assertSame(0, $schema->get('nested.nested.bool')->maxRepetitionsLevel());
        $this->assertSame(5, $schema->get('nested.list_of_ints.list.element')->maxDefinitionsLevel());
        $this->assertSame(1, $schema->get('nested.list_of_ints.list.element')->maxRepetitionsLevel());
    }
}
