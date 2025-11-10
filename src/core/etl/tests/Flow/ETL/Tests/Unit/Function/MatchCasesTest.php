<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, lit, match_cases, match_condition, ref, row, str_entry};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

final class MatchCasesTest extends FlowTestCase
{
    public function test_case_match() : void
    {
        $match = match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ]);

        self::assertSame(
            'this is slug',
            $match->eval(row(str_entry('string', 'this-is-slug')), flow_context())
        );
        self::assertSame(
            'this is slug',
            $match->eval(row(str_entry('string', 'this_is_slug')), flow_context())
        );
    }

    public function test_not_matching_anything_in_lenient_mode() : void
    {
        $match = match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ]);

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::LENIENT);

        self::assertNull($match->eval(row(str_entry('string', 'weirdstring')), $context));
    }

    public function test_not_matching_anything_in_strict_mode() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Not a single case matches row, consider using default parameter, row: {"string":"weirdstring"}');

        $match = match_cases([
            match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ]);

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        $match->eval(row(str_entry('string', 'weirdstring')), $context);
    }

    public function test_not_matching_anything_with_default() : void
    {
        $match = match_cases(
            [
                match_condition(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
                match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
            ],
            default: lit('normal string')
        );

        self::assertEquals(
            'normal string',
            $match->eval(row(str_entry('string', 'weirdstring')), flow_context())
        );
    }
}
