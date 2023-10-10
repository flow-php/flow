<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression\Divide;
use Flow\ETL\Row\Reference\Expression\Minus;
use Flow\ETL\Row\Reference\Expression\Mod;
use Flow\ETL\Row\Reference\Expression\Multiply;
use Flow\ETL\Row\Reference\Expression\Plus;
use Flow\ETL\Row\Reference\Expression\Power;
use Flow\ETL\Row\Reference\Expression\Round;
use PHPUnit\Framework\TestCase;

final class MathTest extends TestCase
{
    public function test_divide() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 10));

        $this->assertSame(
            10,
            (new Divide(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_minus() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 100));

        $this->assertSame(
            0,
            (new Minus(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_modulo() : void
    {
        $row = Row::create(Entry::integer('a', 110), Entry::integer('b', 100));

        $this->assertSame(
            10,
            (new Mod(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_multiple_operations() : void
    {
        $this->assertSame(
            200,
            ref('a')->plus(lit(100))->plus(lit(100))->minus(ref('b'))->eval(Row::create(Entry::int('a', 100), Entry::int('b', 100)))
        );
    }

    public function test_multiply() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 100));

        $this->assertSame(
            10_000,
            (new Multiply(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_plus() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 100));

        $this->assertSame(
            200,
            (new Plus(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_power() : void
    {
        $row = Row::create(Entry::integer('a', 1), Entry::integer('b', 2));

        $this->assertSame(
            1,
            (new Power(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_round() : void
    {
        $row = Row::create(Entry::float('a', 1.009), Entry::integer('b', 2));

        $this->assertSame(
            1.01,
            (new Round(ref('a'), ref('b')))->eval($row)
        );
    }
}
