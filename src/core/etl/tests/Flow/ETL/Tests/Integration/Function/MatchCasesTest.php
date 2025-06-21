<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_rows, lit, match_cases, match_condition, ref, row, rows, string_entry};
use function Flow\Types\DSL\type_integer;
use Flow\ETL\Tests\FlowTestCase;

final class MatchCasesTest extends FlowTestCase
{
    public function test_case_match() : void
    {
        $rows = rows(
            row(string_entry('string', 'string-with-dashes')),
            row(string_entry('string', '123')),
            row(string_entry('string', '14%')),
            row(string_entry('string', '+14')),
            row(string_entry('string', ''))
        );

        $output = df()
            ->read(from_rows($rows))
            ->withEntry(
                'string',
                match_cases(
                    [
                        match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
                        match_condition(ref('string')->call('is_numeric'), ref('string')->cast(type_integer())),
                        match_condition(ref('string')->endsWith('%'), ref('string')->strReplace('%', '')->cast(type_integer())),
                        match_condition(ref('string')->startsWith('+'), ref('string')->strReplace('+', '')->cast(type_integer())),
                    ],
                    default: lit('DEFAULT')
                )
            )
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['string' => 'string with dashes'],
                ['string' => 123],
                ['string' => 14],
                ['string' => 14],
                ['string' => 'DEFAULT'],
            ],
            $output
        );
    }
}
