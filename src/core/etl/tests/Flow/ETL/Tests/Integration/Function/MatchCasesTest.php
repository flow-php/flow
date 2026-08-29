<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\match_cases;
use function Flow\ETL\DSL\match_condition;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;

final class MatchCasesTest extends FlowTestCase
{
    public function test_case_match(): void
    {
        $rows = rows(
            row(string_entry('string', 'string-with-dashes')),
            row(string_entry('string', '123')),
            row(string_entry('string', '14%')),
            row(string_entry('string', '+14')),
            row(string_entry('string', '')),
        );

        $output = df()
            ->read(from_rows($rows))
            ->withEntry('string', match_cases([
                match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
                match_condition(ref('string')->call('is_numeric', type_boolean()), ref('string')->cast(type_integer())),
                match_condition(ref('string')->endsWith('%'), ref('string')->strReplace('%', '')->cast(type_integer())),
                match_condition(
                    ref('string')->startsWith('+'),
                    ref('string')->strReplace('+', '')->cast(type_integer()),
                ),
            ], default: lit('DEFAULT')))
            ->fetch()
            ->toArray();

        static::assertSame(
            [
                ['string' => 'string with dashes'],
                ['string' => '123'],
                ['string' => '14'],
                ['string' => '14'],
                ['string' => 'DEFAULT'],
            ],
            $output,
        );
    }
}
