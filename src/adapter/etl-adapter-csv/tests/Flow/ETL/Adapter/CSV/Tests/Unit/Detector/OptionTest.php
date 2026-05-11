<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit\Detector;

use Flow\ETL\Adapter\CSV\Detector\Option;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class OptionTest extends FlowTestCase
{
    public function test_empty_enclosure(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Option(',', '');
    }

    public function test_empty_separator(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Option('', '"');
    }

    public function test_score(): void
    {
        $option = new Option(',', "'");

        $option->parse('a,b,c');
        $option->parse('a,b,c');
        $option->parse('a,b,c');

        static::assertSame(301000, $option->score());
    }
}
