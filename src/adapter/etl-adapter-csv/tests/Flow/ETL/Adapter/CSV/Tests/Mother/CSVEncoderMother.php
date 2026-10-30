<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Mother;

use Flow\ETL\Adapter\CSV\IO\Loader\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\IO\Loader\PhpCSVEncoder;
use Flow\ETL\Adapter\CSV\IO\Loader\RustCSVEncoder;

final class CSVEncoderMother
{
    public static function native(CSVWriteOptions $options = new CSVWriteOptions(
        newLineSeparator: "\n",
    )): RustCSVEncoder
    {
        return new RustCSVEncoder($options, new PhpCSVEncoder($options));
    }

    public static function php(CSVWriteOptions $options = new CSVWriteOptions(newLineSeparator: "\n")): PhpCSVEncoder
    {
        return new PhpCSVEncoder($options);
    }
}
