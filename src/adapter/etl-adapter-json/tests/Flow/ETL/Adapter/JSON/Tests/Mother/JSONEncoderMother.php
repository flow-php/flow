<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Mother;

use DateTimeInterface;
use Flow\ETL\Adapter\JSON\PhpJSONEncoder;
use Flow\ETL\Adapter\JSON\RustJSONEncoder;

use const JSON_THROW_ON_ERROR;

final class JSONEncoderMother
{
    public static function native(
        int $flags = JSON_THROW_ON_ERROR,
        string $dateTimeFormat = DateTimeInterface::ATOM,
        string $dateFormat = 'Y-m-d',
    ): RustJSONEncoder {
        return new RustJSONEncoder(
            $flags,
            $dateTimeFormat,
            $dateFormat,
            new PhpJSONEncoder($flags, $dateTimeFormat, $dateFormat),
        );
    }

    public static function php(
        int $flags = JSON_THROW_ON_ERROR,
        string $dateTimeFormat = DateTimeInterface::ATOM,
        string $dateFormat = 'Y-m-d',
    ): PhpJSONEncoder {
        return new PhpJSONEncoder($flags, $dateTimeFormat, $dateFormat);
    }
}
