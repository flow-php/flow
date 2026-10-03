<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Php\ListColumnBuilder;
use Flow\ETL\Column\Php\MapColumnBuilder;
use Flow\ETL\Column\Php\NullColumnBuilder;
use Flow\ETL\Column\Php\ScalarColumnBuilder;
use Flow\ETL\Column\Php\StructColumnBuilder;
use Flow\ETL\Column\Physical\PhysicalBuilderFor;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class PhysicalBuilderForTest extends TestCase
{
    public function test_picks_the_builder_of_the_column_kind(): void
    {
        $builders = new PhysicalBuilderFor();

        static::assertInstanceOf(ScalarColumnBuilder::class, $builders->type(type_integer()));
        static::assertInstanceOf(ListColumnBuilder::class, $builders->type(type_optional(type_list(type_integer()))));
        static::assertInstanceOf(MapColumnBuilder::class, $builders->type(type_map(type_string(), type_integer())));
        static::assertInstanceOf(StructColumnBuilder::class, $builders->type(type_structure(['a' => type_integer()])));
        static::assertInstanceOf(NullColumnBuilder::class, $builders->type(type_null()));
    }

    public function test_an_optional_container_keeps_its_optional_type(): void
    {
        $builder = (new PhysicalBuilderFor())->type(type_optional(type_list(type_integer())));
        $builder->appendPhysical(null);

        static::assertEquals(type_optional(type_list(type_integer())), $builder->finish()->type());
    }

    public function test_refuses_a_mixed_element(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('a mixed element has no column kind');

        (new PhysicalBuilderFor())->type(type_list(type_mixed()));
    }
}
