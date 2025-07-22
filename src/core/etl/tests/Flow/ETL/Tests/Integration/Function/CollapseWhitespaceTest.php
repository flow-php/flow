<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class CollapseWhitespaceTest extends FlowTestCase
{
    public function test_collapse_whitespace_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '  Hello    world  '],
                ['text' => 'Multiple   spaces   between   words'],
                ['text' => "Mixed\t\twhitespace\n\ntypes"],
            ]))
            ->withEntry('cleaned_text', ref('text')->collapseWhitespace());

        self::assertEquals(
            [
                ['text' => '  Hello    world  ', 'cleaned_text' => 'Hello world'],
                ['text' => 'Multiple   spaces   between   words', 'cleaned_text' => 'Multiple spaces between words'],
                ['text' => "Mixed\t\twhitespace\n\ntypes", 'cleaned_text' => 'Mixed whitespace types'],
            ],
            $df->fetch()->toArray()
        );
    }
}
