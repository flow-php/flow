<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\String\StringStyles;
use Flow\ETL\Operators\Transformer\Rename\RenameCaseColumnStrategy;
use Flow\ETL\Operators\Transformer\Rename\RenameColumnStrategy;
use Flow\ETL\Operators\Transformer\Rename\RenameMapColumnStrategy;
use Flow\ETL\Operators\Transformer\Rename\RenameReplaceColumnStrategy;
use Flow\ETL\Operators\Transformer\RenameEachColumnTransformer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RenameEachColumnTransformerTest extends FlowTestCase
{
    public function test_bind_feeds_each_strategy_the_output_of_the_previous_one(): void
    {
        static::assertEquals(
            schema(int_schema('uid'), str_schema('user_name')),
            (new RenameEachColumnTransformer(
                new RenameMapColumnStrategy(['id' => 'user_id']),
                new RenameMapColumnStrategy(['user_id' => 'uid', 'name' => 'user_name']),
            ))->bind(schema(int_schema('id'), str_schema('name')))->output,
        );
    }

    public function test_bind_renames_every_column_the_case_strategy_converts(): void
    {
        static::assertEquals(
            schema(int_schema('userId'), str_schema('userName')),
            (new RenameEachColumnTransformer(new RenameCaseColumnStrategy(StringStyles::CAMEL)))->bind(schema(
                int_schema('user_id'),
                str_schema('user_name'),
            ))->output,
        );
    }

    public function test_bind_renames_a_numeric_entry_name(): void
    {
        static::assertEquals(
            schema(str_schema('broker'), int_schema('y_2023')),
            (new RenameEachColumnTransformer(new RenameMapColumnStrategy(['2023' => 'y_2023'])))->bind(schema(
                str_schema('broker'),
                int_schema('2023'),
            ))->output,
        );
    }

    public function test_bind_chains_strategies_through_a_numeric_entry_name(): void
    {
        static::assertEquals(
            schema(int_schema('year')),
            (new RenameEachColumnTransformer(
                new RenameMapColumnStrategy(['id' => '2023']),
                new RenameMapColumnStrategy(['2023' => 'year']),
            ))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_renaming_fails_without_any_strategy(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one strategy must be provided.');

        new RenameEachColumnTransformer();
    }

    public function test_describe_round_trips(): void
    {
        $transformer = new RenameEachColumnTransformer(
            new RenameCaseColumnStrategy(StringStyles::UPPER),
            new RenameMapColumnStrategy(['ID' => 'key']),
            new RenameReplaceColumnStrategy('NA', 'na'),
        );
        $rebuilt = DescribeRoundTrip::rebuilt($transformer);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertSame(
            StepRoundTrip::transformed($transformer, $schema, $rows, flow_context()),
            StepRoundTrip::transformed($rebuilt, $schema, $rows, flow_context()),
        );
    }

    public function test_a_custom_strategy_does_not_describe(): void
    {
        static::assertNull(
            (new RenameEachColumnTransformer(new class implements RenameColumnStrategy {
                public function renames(Schema $schema): array
                {
                    return [];
                }
            }))->describe(),
        );
    }
}
