<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ArrayPushTransformerTest extends TestCase
{
    public function test_array_push_transformer() : void
    {
        $arrayEntry = new ArrayEntry(
            'array',
            [
                'A',
                'Z',
                'C',
                'O',
            ]
        );

        $transformer = Transform::array_push('array', [1]);

        $this->assertSame(
            [
                [
                    'array' => ['A', 'Z', 'C', 'O', 1],
                ],
            ],
            $transformer->transform(new Rows(Row::create($arrayEntry)), new FlowContext(Config::default()))->toArray()
        );
    }

    public function test_array_push_transformer_when_there_is_no_array() : void
    {
        $transformer = Transform::array_push('array', [1]);

        $this->assertSame(
            [
                [
                    'array' => [1],
                ],
            ],
            $transformer->transform(new Rows(Row::create()), new FlowContext(Config::default()))->toArray()
        );
    }
}
