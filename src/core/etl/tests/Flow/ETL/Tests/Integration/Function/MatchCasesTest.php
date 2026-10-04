<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\match_cases;
use function Flow\ETL\DSL\match_condition;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;

final class MatchCasesTest extends FlowTestCase
{
    public function test_case_match(): void
    {
        $rows = array_to_rows([
            ['string' => 'string-with-dashes'],
            ['string' => '123'],
            ['string' => '14%'],
            ['string' => '+14'],
            ['string' => ''],
        ], schema(string_schema('string')));

        $output = df()
            ->read(from_rows($rows))
            ->withEntry('string', match_cases([
                match_condition(ref('string')->contains('-'), ref('string')->strReplace('-', ' ')),
                match_condition(
                    ref('string')->call(lit('is_numeric'), type_boolean()),
                    ref('string')->cast(type_integer()),
                ),
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
