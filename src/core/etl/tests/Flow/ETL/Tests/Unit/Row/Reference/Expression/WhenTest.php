<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression\Literal;
use Flow\ETL\Row\Reference\Expression\When;
use PHPUnit\Framework\TestCase;

final class WhenTest extends TestCase
{
    public function test_condition_not_satisfied_without_else() : void
    {
        $this->assertSame(
            1,
            (new When(
                ref('id')->equals(lit(2)),
                new Literal('then')
            ))->eval(Row::with(Entry::int('id', 1)))
        );
    }

    public function test_else() : void
    {
        $this->assertSame(
            'else',
            (new When(
                new Literal(false),
                new Literal('then'),
                new Literal('else'),
            ))->eval(Row::with(Entry::int('id', 1)))
        );
    }

    public function test_when() : void
    {
        $this->assertSame(
            'then',
            (new When(
                new Literal(true),
                new Literal('then')
            ))->eval(Row::with(Entry::int('id', 1)))
        );
    }
}
