<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{case_, lit, match_, ref, row, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class MatchCasesTest extends FlowTestCase
{
    public function test_case_match() : void
    {
        $match = match_([
            case_(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            case_(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ]);

        self::assertSame(
            'this is slug',
            $match->eval(row(str_entry('string', 'this-is-slug')))
        );
        self::assertSame(
            'this is slug',
            $match->eval(row(str_entry('string', 'this_is_slug')))
        );
    }

    public function test_not_matching_anything() : void
    {
        $match = match_([
            case_(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
            case_(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
        ]);

        $this->expectExceptionMessage('Not a single case matches row, consider using default parameter, row: {"string":"weirdstring"}');

        $match->eval(row(str_entry('string', 'weirdstring')));
    }

    public function test_not_matching_anything_with_default() : void
    {
        $match = match_(
            [
                case_(ref('string')->contains('_'), ref('string')->strReplace('_', ' ')),
                case_(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
            ],
            default: lit('normal string')
        );

        self::assertEquals(
            'normal string',
            $match->eval(row(str_entry('string', 'weirdstring')))
        );
    }
}
