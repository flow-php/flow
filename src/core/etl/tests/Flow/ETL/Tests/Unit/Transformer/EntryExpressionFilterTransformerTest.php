<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\EntryExpressionFilterTransformer;
use PHPUnit\Framework\TestCase;

final class EntryExpressionFilterTransformerTest extends TestCase
{
    public function test_equal() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 1)),
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 1],
            ],
            (new EntryExpressionFilterTransformer(
                ref('a')->equals(ref('b'))
            ))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_equal_on_literal() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 1)),
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new EntryExpressionFilterTransformer(ref('b')->equals(lit(2))))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_greater_than() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new EntryExpressionFilterTransformer(ref('b')->greaterThan(ref('a'))))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_greater_than_or_equal() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 1)),
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            (new EntryExpressionFilterTransformer(ref('b')->greaterThanEqual(ref('a'))))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_less_than() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 1)),
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new EntryExpressionFilterTransformer(ref('a')->lessThan(ref('b'))))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_less_than_equal() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 1)),
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 1],
                ['a' => 1, 'b' => 2],
            ],
            (new EntryExpressionFilterTransformer(ref('a')->lessThanEqual(ref('b'))))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_not_equal() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 1)),
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new EntryExpressionFilterTransformer(ref('a')->notEquals(ref('b'))))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_not_same() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 1)),
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 2],
            ],
            (new EntryExpressionFilterTransformer(ref('a')->notSame(ref('b'))))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_same() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('a', 1), Entry::integer('b', 1)),
            Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
        );

        $this->assertSame(
            [
                ['a' => 1, 'b' => 1],
            ],
            (new EntryExpressionFilterTransformer(ref('a')->same(ref('b'))))->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }
}
