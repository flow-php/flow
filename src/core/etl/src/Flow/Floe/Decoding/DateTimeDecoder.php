<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;

use function ord;
use function sprintf;
use function str_pad;
use function substr;
use function unpack;

use const STR_PAD_LEFT;

final class DateTimeDecoder implements ValueDecoder
{
    public function __construct(
        private readonly TimeZones $timeZones,
    ) {}

    public function decode(string $data, int &$position): DateTimeInterface
    {
        $classFlag = ord($data[$position++]);

        if ($classFlag === Format::DATETIME_IMMUTABLE) {
            $class = DateTimeImmutable::class;
        } elseif ($classFlag === Format::DATETIME_MUTABLE) {
            $class = DateTime::class;
        } else {
            throw new FloeException(sprintf('Floe found unknown datetime flag 0x%02X', $classFlag));
        }

        $timestamp = unpack('P', $data, $position)[1];
        $microseconds = unpack('V', $data, $position + 8)[1];
        $timezoneLength = unpack('V', $data, $position + 12)[1];
        $timezoneName = substr($data, $position + 16, $timezoneLength);
        $position += 16 + $timezoneLength;

        $value = $class::createFromFormat(
            'U.u',
            $timestamp . '.' . str_pad((string) $microseconds, 6, '0', STR_PAD_LEFT),
        );

        if ($value === false) {
            throw new FloeException(sprintf('Floe failed to restore datetime from timestamp "%s"', $timestamp));
        }

        return $value->setTimezone($this->timeZones->get($timezoneName));
    }
}
