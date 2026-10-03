<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Mother;

use DateTimeInterface;
use Flow\ETL\Adapter\JSON\PhpJsonEncoder;
use Flow\ETL\Adapter\JSON\RustJsonEncoder;

use const JSON_THROW_ON_ERROR;

final class JsonEncoderMother
{
    public static function native(
        int $flags = JSON_THROW_ON_ERROR,
        string $dateTimeFormat = DateTimeInterface::ATOM,
        string $dateFormat = 'Y-m-d',
    ): RustJsonEncoder {
        return new RustJsonEncoder(
            $flags,
            $dateTimeFormat,
            $dateFormat,
            new PhpJsonEncoder($flags, $dateTimeFormat, $dateFormat),
        );
    }

    public static function php(
        int $flags = JSON_THROW_ON_ERROR,
        string $dateTimeFormat = DateTimeInterface::ATOM,
        string $dateFormat = 'Y-m-d',
    ): PhpJsonEncoder {
        return new PhpJsonEncoder($flags, $dateTimeFormat, $dateFormat);
    }
}
