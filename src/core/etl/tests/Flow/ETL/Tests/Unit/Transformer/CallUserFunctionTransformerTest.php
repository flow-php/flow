<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class CallUserFunctionTransformerTest extends TestCase
{
    public function test_unique_array() : void
    {
        $callbackTransformer = Transform::user_function(
            ['array_list'],
            'array_unique'
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_list', [1, 1, 1, 2, 3, 4]),
                )
            ),
            new FlowContext(Config::default())
        );

        $callbackTransformer = Transform::user_function(
            ['array_list'],
            'array_values'
        );

        $rows = $callbackTransformer->transform($rows, new FlowContext(Config::default()));

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\ListEntry('array_list', ScalarType::integer, [1, 2, 3, 4]),
            )
        ), $rows);
    }

    public function test_unique_array_with_closure() : void
    {
        $callbackTransformer = Transform::user_function(
            ['array_list'],
            fn (array $entry) => \array_unique($entry)
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_list', [1, 1, 1, 2, 3, 4]),
                )
            ),
            new FlowContext(Config::default())
        );

        $callbackTransformer = Transform::user_function(
            ['array_list'],
            'array_values'
        );

        $rows = $callbackTransformer->transform($rows, new FlowContext(Config::default()));

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\ListEntry('array_list', ScalarType::integer, [1, 2, 3, 4]),
            )
        ), $rows);
    }

    public function test_upper_string_callback() : void
    {
        $callbackTransformer = Transform::user_function(
            ['string-entry'],
            'strtoupper'
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('old-int', 1000),
                    new Entry\StringEntry('string-entry', 'String entry')
                )
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('old-int', 1000),
                new Entry\StringEntry('string-entry', 'STRING ENTRY')
            )
        ), $rows);
    }
}
