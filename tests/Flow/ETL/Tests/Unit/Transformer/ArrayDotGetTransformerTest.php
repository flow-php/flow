<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ArrayDotGetTransformerTest extends TestCase
{
    public function test_array_access_for_not_array_entry() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('integer_entry is not ArrayEntry but Flow\ETL\Row\Entry\IntegerEntry');

        $arrayUnpackTransformer = Transform::array_get('integer_entry', 'invalid_path');

        $arrayUnpackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('integer_entry', 1),
                ),
            ),
            new FlowContext(Config::default())
        );
    }

    public function test_array_accessor_transformer() : void
    {
        $arrayAccessorTransformer = Transform::array_get('array_entry', 'array.foo');

        $rows = $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ]),
                ),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            new Row\Entry\StringEntry('element', 'bar'),
            $rows->first()->get('element')
        );
    }

    public function test_array_accessor_transformer_with_invalid_and_without_strict_path() : void
    {
        $arrayAccessorTransformer = Transform::array_get('array_entry', '?invalid_path');

        $rows = $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'datetime' => new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                        'array' => ['foo' => 'bar'],
                    ]),
                ),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            new Row\Entry\NullEntry('element'),
            $rows->first()->get('element')
        );
    }

    public function test_array_accessor_transformer_with_invalid_but_strict_path() : void
    {
        $arrayAccessorTransformer = Transform::array_get('array_entry', 'invalid_path');

        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        'id' => 1,
                        'status' => 'PENDING',
                        'enabled' => true,
                        'datetime' => new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                        'array' => ['foo' => 'bar'],
                    ]),
                ),
            ),
            new FlowContext(Config::default())
        );
    }
}
