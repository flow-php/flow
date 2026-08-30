<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

use const STR_PAD_LEFT;

final class StrPadTest extends FlowTestCase
{
    public function test_str_pad_on_valid_string(): void
    {
        static::assertSame('----N', ref('value')
            ->strPad(5, '-', STR_PAD_LEFT)
            ->eval(row(['value' => 'N']), flow_context()));
    }
}
