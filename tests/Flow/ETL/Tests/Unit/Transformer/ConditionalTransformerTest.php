<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Condition;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\StaticEntryTransformer;
use PHPUnit\Framework\TestCase;

final class ConditionalTransformerTest extends TestCase
{
    public function test_transformation_when_condition_met_for_one_row_in_rows() : void
    {
        $transformer = Transform::chain(
            Transform::transform_if(
                Condition::all(
                    Condition::equals_to_value('first_name', 'Michael'),
                    Condition::equals_to_value('last_name', 'Jackson'),
                ),
                new StaticEntryTransformer(new Row\Entry\StringEntry('profession', 'singer'))
            ),
            Transform::transform_if(
                Condition::all(
                    Condition::equals_to_value('first_name', 'Rocky'),
                    Condition::equals_to_value('last_name', 'Balboa'),
                ),
                new StaticEntryTransformer(new Row\Entry\StringEntry('profession', 'boxer'))
            )
        );

        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('first_name', 'Michael'),
                new Row\Entry\StringEntry('last_name', 'Jackson'),
            ),
            Row::create(
                new Row\Entry\IntegerEntry('id', 2),
                new Row\Entry\StringEntry('first_name', 'Rocky'),
                new Row\Entry\StringEntry('last_name', 'Balboa'),
            )
        );

        $this->assertSame(
            [
                [
                    'id' => 1,
                    'first_name' => 'Michael',
                    'last_name' => 'Jackson',
                    'profession' => 'singer',
                ],
                [
                    'id' => 2,
                    'first_name' => 'Rocky',
                    'last_name' => 'Balboa',
                    'profession' => 'boxer',
                ],
            ],
            $transformer->transform($rows)->toArray()
        );
    }
}
