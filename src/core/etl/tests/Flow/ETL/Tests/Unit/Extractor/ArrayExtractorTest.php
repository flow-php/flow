<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\execution_context;
use function Flow\ETL\DSL\from_array;

final class ArrayExtractorTest extends FlowTestCase
{
    public function test_array_extractor(): void
    {
        $extractor = from_array([
            ['id' => 1, 'name' => 'Norbert'],
            ['id' => 2, 'name' => 'Michal'],
        ]);

        $rows = \iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        static::assertCount(2, $rows);
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        static::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }

    public function test_generator_extraction(): void
    {
        $generator = static function () {
            yield ['id' => 1, 'name' => 'Norbert'];
            yield ['id' => 2, 'name' => 'Michal'];
        };

        $extractor = from_array($generator());

        $rows = \iterator_to_array($extractor->extract(execution_context(config())));

        static::assertCount(2, $rows);
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        static::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }
}
