<?php

declare(strict_types=1);

namespace Flow\Serializer\Tests\Unit;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class CompressingSerializerTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\function_exists('gzcompress')) {
            $this->markTestSkipped('gzcompress unavailable.');
        }
    }

    public function test_serializing_rows() : void
    {
        $rows = new Rows(
            ...\array_map(
                fn () : Row => Row::create(
                    Entry::integer('integer', 1),
                    Entry::string('string', 'string'),
                    Entry::boolean('boolean', true),
                    Entry::datetime('datetime', new \DateTimeImmutable('2022-01-01 00:00:00')),
                    Entry::null('null'),
                    Entry::float('float', 0.12),
                    Entry::object('object', new \ArrayIterator([1, 2, 3])),
                    Entry::structure(
                        'struct',
                        Entry::integer('integer', 1),
                        Entry::string('string', 'string'),
                    )
                ),
                \range(0, 100)
            )
        );

        $serializer = new CompressingSerializer(new NativePHPSerializer());

        $serialized = $serializer->serialize($rows);

        $unserialized = $serializer->unserialize($serialized);

        $this->assertEquals(
            $rows,
            $unserialized
        );
    }
}
