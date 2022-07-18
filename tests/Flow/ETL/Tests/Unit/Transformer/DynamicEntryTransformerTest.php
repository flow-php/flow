<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class DynamicEntryTransformerTest extends TestCase
{
    public function test_adding_new_entries() : void
    {
        $transformer = Transform::dynamic(
            fn (Row $row) : Row\Entries => new Row\Entries(new Row\Entry\DateTimeEntry('updated_at', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')))
        );

        $rows = $transformer->transform(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 1)),
            Row::create(new Row\Entry\IntegerEntry('id', 2)),
        ), new FlowContext(Config::default()));

        $this->assertEquals(
            [
                ['id' => 1, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
                ['id' => 2, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_new_entries_with_serialization() : void
    {
        $transformer = Transform::dynamic(
            fn (Row $row) : Row\Entries => new Row\Entries(new Row\Entry\DateTimeEntry('updated_at', new \DateTimeImmutable('2020-01-01 00:00:00 UTC')))
        );

        $serializer = new NativePHPSerializer();

        $rows = $serializer->unserialize($serializer->serialize($transformer))->transform(new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 1)),
            Row::create(new Row\Entry\IntegerEntry('id', 2)),
        ), new FlowContext(Config::default()));

        $this->assertEquals(
            [
                ['id' => 1, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
                ['id' => 2, 'updated_at' => new \DateTimeImmutable('2020-01-01T00:00:00+00:00')],
            ],
            $rows->toArray()
        );
    }
}
