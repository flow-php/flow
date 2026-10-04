<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Double\FailingOnValuesFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\match_cases;
use function Flow\ETL\DSL\match_condition;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class MatchCasesTest extends FlowTestCase
{
    public function test_no_matching_case_without_a_default_still_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Not a single case matches row, consider using default parameter');

        (new FunctionContext(flow_context()))->eval(
            match_cases([
                match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            ]),
            ['string' => 'weirdstring'],
            schema(str_schema('string')),
        );
    }

    public function test_case_match(): void
    {
        $match = match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ]);

        static::assertSame('this is slug', (new FunctionContext(flow_context()))->eval(
            $match,
            [
                'string' => 'this-is-slug',
            ],
            schema(str_schema('string')),
        ));
        static::assertSame('this is slug', (new FunctionContext(flow_context()))->eval(
            $match,
            [
                'string' => 'this_is_slug',
            ],
            schema(str_schema('string')),
        ));
    }

    public function test_not_matching_anything_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Not a single case matches row, consider using default parameter, row: {"string":"weirdstring"}',
        );

        $match = match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ]);

        $context = flow_context(config());
        (new FunctionContext($context))->eval($match, ['string' => 'weirdstring'], schema(str_schema('string')));
    }

    public function test_not_matching_anything_with_default(): void
    {
        $match = match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ], default: lit('normal string'));

        static::assertEquals('normal string', (new FunctionContext(flow_context()))->eval(
            $match,
            [
                'string' => 'weirdstring',
            ],
            schema(str_schema('string')),
        ));
    }

    public function test_a_later_case_runs_only_on_the_rows_no_earlier_case_took(): void
    {
        $rows = array_to_rows(
            [['c' => true, 'v' => 99], ['c' => false, 'v' => 2]],
            schema(bool_schema('c'), int_schema('v')),
        );

        static::assertSame(
            [0, 2],
            (new ReferenceResolver())
                ->resolve(match_cases([
                    match_condition(ref('c'), lit(0)),
                    match_condition((new FailingOnValuesFunction(ref('v'), [99]))->isNotNull(), ref('v')),
                ]), $rows->schema())
                ->eval($rows, flow_context())
                ->values(),
        );
    }
}
