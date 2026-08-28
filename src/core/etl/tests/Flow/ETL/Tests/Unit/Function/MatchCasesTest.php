<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\match_cases;
use function Flow\ETL\DSL\match_condition;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class MatchCasesTest extends FlowTestCase
{
    public function test_no_matching_case_without_a_default_still_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Not a single case matches row, consider using default parameter');

        match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
        ])->eval(row(str_entry('string', 'weirdstring')), flow_context());
    }

    public function test_case_match(): void
    {
        $match = match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ]);

        static::assertSame('this is slug', $match->eval(row(str_entry('string', 'this-is-slug')), flow_context()));
        static::assertSame('this is slug', $match->eval(row(str_entry('string', 'this_is_slug')), flow_context()));
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
        $match->eval(row(str_entry('string', 'weirdstring')), $context);
    }

    public function test_not_matching_anything_with_default(): void
    {
        $match = match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ], default: lit('normal string'));

        static::assertEquals('normal string', $match->eval(row(str_entry('string', 'weirdstring')), flow_context()));
    }
}
