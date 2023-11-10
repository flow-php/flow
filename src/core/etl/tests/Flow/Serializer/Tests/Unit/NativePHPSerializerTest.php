<?php

declare(strict_types=1);

namespace Flow\Serializer\Tests\Unit;

use Flow\ETL\DSL\Entry;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

final class NativePHPSerializerTest extends TestCase
{
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
                        ['integer' => 1, 'string' => 'string'],
                        new StructureType(
                            new StructureElement('integer', ScalarType::integer()),
                            new StructureElement('string', ScalarType::string())
                        )
                    )
                ),
                \range(0, 100)
            )
        );

        $serializer = new NativePHPSerializer();

        $serialized = $serializer->serialize($rows);

        $unserialized = $serializer->unserialize($serialized);

        $this->assertEquals(
            $rows,
            $unserialized
        );
    }
}
