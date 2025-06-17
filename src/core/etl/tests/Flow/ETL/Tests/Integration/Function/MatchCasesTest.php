<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{case_, df, from_rows, match_, ref, row, rows, string_entry};
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
            row(string_entry('string', '+14'))
        );

        $output = df()
            ->read(from_rows($rows))
            ->withEntry(
                'string',
                match_([
                    case_(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
                    case_(ref('string')->call('is_numeric'), ref('string')->cast(type_integer())),
                    case_(ref('string')->endsWith('%'), ref('string')->strReplace('%', '')->cast(type_integer())),
                    case_(ref('string')->startsWith('+'), ref('string')->strReplace('+', '')->cast(type_integer())),
                ])
            )
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['string' => 'string with dashes'],
                ['string' => 123],
                ['string' => 14],
                ['string' => 14],
            ],
            $output
        );
    }
}
