<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use DateTimeImmutable;
use Flow\ETL\Transformer\AutoCastTransformer;
use Flow\Types\Type\AutoCaster;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AutoCastTransformerTest extends TestCase
{
    public function test_transforming_row(): void
    {
        $transformer = new AutoCastTransformer(new AutoCaster());

        $rows = array_to_rows(
            [
                [
                    'integer' => '1',
                    'float' => '1.0',
                    'boolean' => 'true',
                    'json' => '{"foo":"bar"}',
                    'datetime' => '2021-01-01 00:00:00',
                    'null' => 'null',
                    'nil' => 'nil',
                ],
            ],
            schema(
                str_schema('integer'),
                str_schema('float'),
                str_schema('boolean'),
                str_schema('json'),
                str_schema('datetime'),
                str_schema('null'),
                str_schema('nil'),
            ),
            flow_context(config())->hydrator(),
        );

        static::assertEquals(
            [
                [
                    'integer' => 1,
                    'float' => 1.0,
                    'boolean' => true,
                    'json' => ['foo' => 'bar'],
                    'datetime' => new DateTimeImmutable('2021-01-01 00:00:00'),
                    'null' => null,
                    'nil' => null,
                ],
            ],
            $transformer->transform($rows, flow_context())->toArray(),
        );
    }
}
