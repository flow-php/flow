<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{config, config_builder, execution_context, from_array};
use PHPUnit\Framework\TestCase;

final class ArrayExtractorTest extends TestCase
{
    public function test_array_extractor() : void
    {
        $extractor = from_array([
            ['id' => 1, 'name' => 'Norbert'],
            ['id' => 2, 'name' => 'Michal'],
        ]);

        $rows = \iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        self::assertCount(2, $rows);
        self::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        self::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }

    public function test_generator_extraction() : void
    {
        $generator = function () {
            yield ['id' => 1, 'name' => 'Norbert'];
            yield ['id' => 2, 'name' => 'Michal'];
        };

        $extractor = from_array($generator());

        $rows = \iterator_to_array($extractor->extract(execution_context(config())));

        self::assertCount(2, $rows);
        self::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        self::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }
}
