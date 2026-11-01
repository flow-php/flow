<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Explain;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RenderedNodeMother;

final class RenderedNodeTest extends FlowTestCase
{
    public function test_title_puts_the_number_before_the_name(): void
    {
        static::assertSame('#3 Read', RenderedNodeMother::named('Read', 3)->title());
    }

    public function test_title_of_an_entry_without_a_number_is_the_name(): void
    {
        static::assertSame('Outputs', RenderedNodeMother::named('Outputs', null)->title());
    }

    public function test_a_shared_entry_drops_the_details_the_first_visit_printed(): void
    {
        $shared = RenderedNodeMother::named(
            'Collect',
            2,
            lines: ['Buffers all rows before passing them on'],
            suffix: 'blocking',
        )->with([], shared: true);

        static::assertSame([], $shared->lines);
        static::assertSame('', $shared->suffix);
        static::assertTrue($shared->shared);
        static::assertSame('#2 Collect', $shared->title());
    }
}
