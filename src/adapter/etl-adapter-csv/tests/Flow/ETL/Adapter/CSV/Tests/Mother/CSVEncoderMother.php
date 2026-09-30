<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Mother;

use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\NativeCSVEncoder;
use Flow\ETL\Adapter\CSV\NativeCSVWriter;
use Flow\ETL\Adapter\CSV\PhpCSVEncoder;

final class CSVEncoderMother
{
    public static function native(CSVWriteOptions $options = new CSVWriteOptions(
        newLineSeparator: "\n",
    )): NativeCSVEncoder
    {
        return new NativeCSVEncoder(self::writer($options), new PhpCSVEncoder($options));
    }

    public static function php(CSVWriteOptions $options = new CSVWriteOptions(newLineSeparator: "\n")): PhpCSVEncoder
    {
        return new PhpCSVEncoder($options);
    }

    public static function writer(CSVWriteOptions $options = new CSVWriteOptions(
        newLineSeparator: "\n",
    )): NativeCSVWriter
    {
        return new NativeCSVWriter(
            $options->separator,
            $options->enclosure,
            $options->escape,
            $options->newLineSeparator,
            $options->dateTimeFormat,
            $options->dateFormat,
        );
    }
}
