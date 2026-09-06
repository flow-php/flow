<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use DateTime;
use DateTimeImmutable;
use Flow\Floe\Exception\FloeException;

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
        // The class is not stored: a datetime column always hydrates to DateTimeImmutable, so a
        // flag distinguishing the two would describe nothing a reader can observe. Custom
        // subclasses are still refused - their extra state would be silently dropped.
        if ($value::class !== DateTimeImmutable::class && $value::class !== DateTime::class) {
            throw new FloeException(sprintf(
                'Floe supports only DateTime and DateTimeImmutable, got %s - convert custom datetime instances before writing',
                $value::class,
            ));
        }

        $timezone = $value->getTimezone()->getName();

        return (
            pack('P', $value->getTimestamp())
            . pack('V', (int) $value->format('u'))
            . pack('V', strlen($timezone))
            . $timezone
        );
    }
}
