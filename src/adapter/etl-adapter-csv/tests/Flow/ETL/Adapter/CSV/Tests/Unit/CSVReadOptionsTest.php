<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVReadOptions;
use PHPUnit\Framework\TestCase;

final class CSVReadOptionsTest extends TestCase
{
    public function test_a_wither_returns_a_new_instance(): void
    {
        $options = new CSVReadOptions();

        static::assertNotSame($options, $options->withSeparator(';'));
        static::assertNull($options->separator);
    }

    /**
     * Each expected value is fully constructed, so the six knobs the wither must NOT touch are asserted by
     * construction rather than by looping over property names.
     */
    public function test_each_wither_changes_only_its_own_knob(): void
    {
        $options = new CSVReadOptions();

        static::assertEquals(new CSVReadOptions(charactersReadInLine: 2000), $options->withCharactersReadInLine(2000));
        static::assertEquals(new CSVReadOptions(emptyToNull: false), $options->withEmptyToNull(false));
        static::assertEquals(new CSVReadOptions(enclosure: "'"), $options->withEnclosure("'"));
        static::assertEquals(new CSVReadOptions(escape: '/'), $options->withEscape('/'));
        static::assertEquals(new CSVReadOptions(withHeader: false), $options->withHeader(false));
        static::assertEquals(new CSVReadOptions(removeBOM: false), $options->withRemoveBOM(false));
        static::assertEquals(new CSVReadOptions(separator: ';'), $options->withSeparator(';'));
    }

    public function test_the_defaults_are_the_extractors_own(): void
    {
        $options = new CSVReadOptions();

        static::assertTrue($options->withHeader);
        static::assertTrue($options->emptyToNull);
        static::assertTrue($options->removeBOM);
        static::assertNull($options->separator);
        static::assertNull($options->enclosure);
        static::assertNull($options->escape);
        static::assertNull($options->charactersReadInLine);
    }

    public function test_the_withers_compose(): void
    {
        static::assertEquals(
            new CSVReadOptions(withHeader: false, separator: ';', escape: '/'),
            (new CSVReadOptions())
                ->withHeader(false)
                ->withSeparator(';')
                ->withEscape('/'),
        );
    }
}
