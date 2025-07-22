<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class WordwrapTest extends FlowTestCase
{
    public function test_wordwrap_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'The quick brown fox jumps over the lazy dog', 'width' => 15],
                ['text' => 'Hello World Test', 'width' => 10],
                ['text' => 'PHP is awesome', 'width' => 8],
            ]))
            ->withEntry('wrapped_text', ref('text')->wordwrap(ref('width')));

        self::assertEquals(
            [
                ['text' => 'The quick brown fox jumps over the lazy dog', 'width' => 15, 'wrapped_text' => "The quick brown\nfox jumps over\nthe lazy dog"],
                ['text' => 'Hello World Test', 'width' => 10, 'wrapped_text' => "Hello\nWorld Test"],
                ['text' => 'PHP is awesome', 'width' => 8, 'wrapped_text' => "PHP is\nawesome"],
            ],
            $df->fetch()->toArray()
        );
    }
}
