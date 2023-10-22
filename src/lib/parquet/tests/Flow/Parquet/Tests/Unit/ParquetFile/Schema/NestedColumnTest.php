<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema;
use PHPUnit\Framework\TestCase;

final class NestedColumnTest extends TestCase
{
    public function test_children_flat() : void
    {
        $column = Schema\NestedColumn::create('nested', [
            Schema\NestedColumn::create('nested', [
                Schema\NestedColumn::create('nested', [
                    Schema\FlatColumn::int32('int'),
                    Schema\FlatColumn::string('string'),
                    Schema\FlatColumn::boolean('bool'),
                ]),
            ]),
        ]);

        $this->assertSame(
            [
                'nested.nested.nested.int',
                'nested.nested.nested.string',
                'nested.nested.nested.bool',
            ],
            \array_keys($column->childrenFlat())
        );
    }

    public function test_flat_path_for_direct_root_child() : void
    {
        $schema = Schema::with(
            Schema\FlatColumn::int32('int'),
            Schema\FlatColumn::string('string'),
            Schema\FlatColumn::boolean('bool'),
        );

        $this->assertSame('int', $schema->get('int')->flatPath());
        $this->assertSame('string', $schema->get('string')->flatPath());
        $this->assertSame('bool', $schema->get('bool')->flatPath());
    }
}
