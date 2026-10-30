<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\All;
use Flow\ETL\Functions\Any;
use Flow\ETL\Functions\Conjuncts;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class ConjunctsTest extends FlowTestCase
{
    public function test_a_function_that_is_not_an_and_is_its_only_conjunct(): void
    {
        $function = ref('id')->greaterThan(lit(1));

        static::assertSame([$function], (new Conjuncts())->of($function));
    }

    public function test_a_nested_and_is_flattened(): void
    {
        $a = ref('a')->equals(lit(1));
        $b = ref('b')->equals(lit(2));
        $c = ref('c')->equals(lit(3));
        $d = ref('d')->equals(lit(4));

        static::assertSame([$a, $b, $c, $d], (new Conjuncts())->of(new All($a, new All($b, new All($c)), $d)));
    }

    public function test_an_or_is_kept_whole(): void
    {
        $a = ref('a')->equals(lit(1));
        $or = new Any(ref('b')->equals(lit(2)), new All(ref('c')->equals(lit(3)), ref('d')->equals(lit(4))));

        static::assertSame([$a, $or], (new Conjuncts())->of(new All($a, $or)));
    }
}
