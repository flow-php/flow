<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class CallUserFunctionTransformerTest extends TestCase
{
    public function test_ceil() : void
    {
        $transformer = Transform::ceil('float');

        $rows = $transformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\FloatEntry('float', 10.54613),
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\FloatEntry('float', 11),
            )
        ), $rows);
    }

    public function test_floor() : void
    {
        $transformer = Transform::floor('float');

        $rows = $transformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\FloatEntry('float', 10.54613),
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\FloatEntry('float', 10),
            )
        ), $rows);
    }

    public function test_ltrim() : void
    {
        $callbackTransformer = Transform::ltrim('string');

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Entry\StringEntry('string', '  Something  ')
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Entry\StringEntry('string', 'Something  ')
            )
        ), $rows);
    }

    public function test_preg_replace_callback() : void
    {
        $callbackTransformer = Transform::preg_replace('string', '/^\[[0-9]+\][[0-9]+\]/', '');

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Entry\StringEntry('string', '[90321][90346]/Frodo...')
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Entry\StringEntry('string', '/Frodo...')
            )
        ), $rows);
    }

    public function test_round() : void
    {
        $transformer = Transform::round('float', 2, \PHP_ROUND_HALF_DOWN);

        $rows = $transformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\FloatEntry('float', 10.54613),
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\FloatEntry('float', 10.55),
            )
        ), $rows);
    }

    public function test_rtrim() : void
    {
        $callbackTransformer = Transform::rtrim('string');

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Entry\StringEntry('string', '  Something  ')
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Entry\StringEntry('string', '  Something')
            )
        ), $rows);
    }

    public function test_str_pad() : void
    {
        $callbackTransformer = Transform::str_pad(
            'string',
            5,
            '-',
            \STR_PAD_LEFT
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\StringEntry('string', 'N'),
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Entry\StringEntry('string', '----N')
            )
        ), $rows);
    }

    public function test_str_replace_callback() : void
    {
        $callbackTransformer = Transform::str_replace('string', 'thing', '');

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Entry\StringEntry('string', 'Something')
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Entry\StringEntry('string', 'Some')
            )
        ), $rows);
    }

    public function test_trim() : void
    {
        $callbackTransformer = Transform::trim('string');

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Entry\StringEntry('string', '  Something  ')
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Entry\StringEntry('string', 'Something')
            )
        ), $rows);
    }

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
            )
        );

        $callbackTransformer = Transform::user_function(
            ['array_list'],
            'array_values'
        );

        $rows = $callbackTransformer->transform($rows);

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
            )
        );

        $callbackTransformer = Transform::user_function(
            ['array_list'],
            'array_values'
        );

        $rows = $callbackTransformer->transform($rows);

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
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('old-int', 1000),
                new Entry\StringEntry('string-entry', 'STRING ENTRY')
            )
        ), $rows);
    }
}
