<?php

declare(strict_types=1);

namespace Flow\Serializer\Tests\Unit;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Rows;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class CompressingSerializerTest extends TestCase
{
    public function setUp() : void
    {
        if (!\function_exists('gzcompress')) {
            $this->markTestSkipped('gzcompress unavailable.');
        }
    }

    public function test_serializing_rows() : void
    {
        $rows = new Rows(
            ...\array_map(
                function () : Row {
                    return Row::create(
                        new IntegerEntry('integer', 1),
                        new StringEntry('string', 'string'),
                        new BooleanEntry('boolean', true),
                        new DateTimeEntry('datetime', new \DateTimeImmutable('2022-01-01 00:00:00')),
                        new NullEntry('null'),
                        new FloatEntry('float', 0.12),
                        new ObjectEntry('object', new \ArrayIterator([1, 2, 3])),
                        new StructureEntry(
                            'struct',
                            new IntegerEntry('integer', 1),
                            new StringEntry('string', 'string'),
                        )
                    );
                },
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
