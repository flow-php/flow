<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Mother;

use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\PhpCSVEncoder;
use Flow\ETL\Adapter\CSV\RustCSVEncoder;

final class CSVEncoderMother
{
    public static function native(CSVWriteOptions $options = new CSVWriteOptions(
        newLineSeparator: "\n",
    )): RustCSVEncoder
    {
        return new RustCSVEncoder(
            $options->separator,
            $options->enclosure,
            $options->escape,
            $options->newLineSeparator,
            $options->dateTimeFormat,
            $options->dateFormat,
            new PhpCSVEncoder($options),
        );
    }

    public static function php(CSVWriteOptions $options = new CSVWriteOptions(newLineSeparator: "\n")): PhpCSVEncoder
    {
        return new PhpCSVEncoder($options);
    }
}
