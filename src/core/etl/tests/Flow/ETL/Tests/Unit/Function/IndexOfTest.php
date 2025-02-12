<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_int};
use Flow\ETL\Function\IndexOf;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class IndexOfTest extends FlowTestCase
{
    public function test_index_of() : void
    {
        self::assertSame(
            2,
            ref('str')->indexOf('B')->eval(
                row(str_entry('str', 'AbBA'))
            )
        );

        self::assertSame(
            null,
            ref('str')->indexOf('B')->eval(
                row(str_entry('str', 'Abba'))
            )
        );
    }

    public function test_returns_method_returns_string_int() : void
    {
        $indexOf = new IndexOf('Abba', 'B');
        $returnType = $indexOf->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_int()));
    }
}
