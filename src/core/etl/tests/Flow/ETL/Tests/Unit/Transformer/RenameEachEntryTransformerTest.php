<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\Rename\RenameCaseEntryStrategy;
use Flow\ETL\Transformer\Rename\RenameMapEntryStrategy;
use Flow\ETL\Transformer\RenameEachEntryTransformer;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RenameEachEntryTransformerTest extends FlowTestCase
{
    public function test_bind_feeds_each_strategy_the_output_of_the_previous_one(): void
    {
        static::assertEquals(
            schema(int_schema('uid'), str_schema('user_name')),
            (new RenameEachEntryTransformer(
                new RenameMapEntryStrategy(['id' => 'user_id']),
                new RenameMapEntryStrategy(['user_id' => 'uid', 'name' => 'user_name']),
            ))->bind(schema(int_schema('id'), str_schema('name')))->output,
        );
    }

    public function test_bind_renames_every_column_the_case_strategy_converts(): void
    {
        static::assertEquals(
            schema(int_schema('userId'), str_schema('userName')),
            (new RenameEachEntryTransformer(new RenameCaseEntryStrategy(StringStyles::CAMEL)))->bind(schema(
                int_schema('user_id'),
                str_schema('user_name'),
            ))->output,
        );
    }

    public function test_renaming_fails_without_any_strategy(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one strategy must be provided.');

        new RenameEachEntryTransformer();
    }
}
