<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use DateTime;
use DateTimeImmutable;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;

use function chr;
use function pack;
use function sprintf;
use function strlen;

/**
 * @implements ValueEncoder<\DateTimeInterface>
 */
final class DateTimeEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        $head = match ($value::class) {
            DateTimeImmutable::class => chr(Format::DATETIME_IMMUTABLE),
            DateTime::class => chr(Format::DATETIME_MUTABLE),
            default => throw new FloeException(sprintf(
                'Floe supports only DateTime and DateTimeImmutable, got %s - convert custom datetime instances before writing',
                $value::class,
            )),
        };

        $timezone = $value->getTimezone()->getName();

        return (
            $head
            . pack('P', $value->getTimestamp())
            . pack('V', (int) $value->format('u'))
            . pack('V', strlen($timezone))
            . $timezone
        );
    }
}
