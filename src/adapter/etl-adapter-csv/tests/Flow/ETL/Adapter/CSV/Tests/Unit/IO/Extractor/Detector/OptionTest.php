<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit\IO\Extractor\Detector;

use Flow\ETL\Adapter\CSV\IO\Extractor\Detector\Option;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
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

    public function test_a_multibyte_separator_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new Option('¦', '"');
    }

    public function test_an_escape_of_more_than_one_character_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Escape must be empty or a single character');

        new Option(',', '"', 'ab');
    }

    public function test_score_counts_bytes(): void
    {
        $multibyte = new Option(',', '"');
        $multibyte->parse('é,é');
        $ascii = new Option(',', '"');
        $ascii->parse('ab,ab');

        static::assertSame($ascii->score(), $multibyte->score());
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
