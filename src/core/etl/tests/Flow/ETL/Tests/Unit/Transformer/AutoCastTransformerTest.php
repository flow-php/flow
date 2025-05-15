<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{array_to_rows, flow_context};
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\AutoCastTransformer;
use Flow\Types\Type\{AutoCaster};

final class AutoCastTransformerTest extends FlowTestCase
{
    public function test_transforming_row() : void
    {
        $transformer = new AutoCastTransformer(new AutoCaster());

        $rows = array_to_rows([
            [
                'integer' => '1',
                'float' => '1.0',
                'boolean' => 'true',
                'json' => '{"foo":"bar"}',
                'datetime' => '2021-01-01 00:00:00',
                'null' => 'null',
                'nil' => 'nil',
            ],
        ]);

        self::assertEquals(
            [
                [
                    'integer' => 1,
                    'float' => 1.0,
                    'boolean' => true,
                    'json' => ['foo' => 'bar'],
                    'datetime' => new \DateTimeImmutable('2021-01-01 00:00:00'),
                    'null' => null,
                    'nil' => null,
                ],
            ],
            $transformer->transform($rows, flow_context())->toArray()
        );
    }
}
