<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{int_entry, str_entry};
use Flow\ETL\Join\Expression;
use Flow\ETL\Transformer\JoinRowsTransformer;
use Flow\ETL\{Config, Flow, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class JoinRowsTransformerTest extends TestCase
{
    public function test_inner_join_rows() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'US')),
            Row::create(int_entry('id', 3), str_entry('country', 'FR')),
        );
        $right = (new Flow())->process(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            )
        );

        $transformer = JoinRowsTransformer::inner($right, Expression::on(['country' => 'code']));

        self::assertEquals(
            new Rows(
                Row::create(str_entry('name', 'Poland'), int_entry('id', 1), str_entry('country', 'PL')),
                Row::create(str_entry('name', 'United States'), int_entry('id', 2), str_entry('country', 'US')),
            ),
            $transformer->transform($left, new FlowContext(Config::default()))
        );
    }

    public function test_left_join_rows() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'US')),
            Row::create(int_entry('id', 3), str_entry('country', 'FR')),
        );
        $right = (new Flow())->process(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            )
        );

        $transformer = JoinRowsTransformer::left($right, Expression::on(['country' => 'code']));

        self::assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 2), str_entry('country', 'US'), str_entry('name', 'United States')),
                Row::create(int_entry('id', 3), str_entry('country', 'FR'), str_entry('name', null)),
            ),
            $transformer->transform($left, new FlowContext(Config::default()))
        );
    }

    public function test_right_join_rows() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'US')),
            Row::create(int_entry('id', 3), str_entry('country', 'FR')),
        );
        $right = (new Flow())->process(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            )
        );

        $transformer = JoinRowsTransformer::right($right, Expression::on(['country' => 'code']));

        self::assertEquals(
            new Rows(
                Row::create(str_entry('name', 'Poland'), str_entry('code', 'PL'), int_entry('id', 1)),
                Row::create(str_entry('name', 'United States'), str_entry('code', 'US'), int_entry('id', 2)),
                Row::create(str_entry('name', 'Great Britain'), str_entry('code', 'GB'), str_entry('id', null)),
            ),
            $transformer->transform($left, new FlowContext(Config::default()))
        );
    }
}
