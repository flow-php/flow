<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use DateTimeInterface;
use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class CSVWriteOptionsTest extends TestCase
{
    public function test_defaults(): void
    {
        $options = new CSVWriteOptions();

        static::assertSame([',', '"', '\\', PHP_EOL, DateTimeInterface::ATOM, 'Y-m-d'], [
            $options->separator,
            $options->enclosure,
            $options->escape,
            $options->newLineSeparator,
            $options->dateTimeFormat,
            $options->dateFormat,
        ]);
    }

    public function test_an_empty_escape_is_accepted(): void
    {
        static::assertSame('', (new CSVWriteOptions(escape: ''))->escape);
    }

    #[TestWith(['', '"', '\\', 'Separator must be a single character'])]
    #[TestWith([';;', '"', '\\', 'Separator must be a single character'])]
    #[TestWith([',', '', '\\', 'Enclosure must be a single character'])]
    #[TestWith([',', '""', '\\', 'Enclosure must be a single character'])]
    #[TestWith([',', '"', '\\\\', 'Escape must be empty or a single character'])]
    public function test_a_malformed_dialect_is_refused(
        string $separator,
        string $enclosure,
        string $escape,
        string $message,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        new CSVWriteOptions($separator, $enclosure, $escape);
    }
}
