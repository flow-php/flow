<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\Floe\Exception\FloeException;

use function sprintf;
use function str_pad;
use function strlen;
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
        // unpack() raises a ValueError past the end of the buffer, which is not a Floe error
        if (strlen($data) < ($position + 16)) {
            throw new FloeException('Floe found a truncated datetime value');
        }

        $timestamp = unpack('P', $data, $position)[1];
        $microseconds = unpack('V', $data, $position + 8)[1];
        $timezoneLength = unpack('V', $data, $position + 12)[1];
        if (strlen($data) < ($position + 16 + $timezoneLength)) {
            throw new FloeException('Floe found a truncated datetime value');
        }

        $timezoneName = substr($data, $position + 16, $timezoneLength);
        $position += 16 + $timezoneLength;

        $value = DateTimeImmutable::createFromFormat(
            'U.u',
            $timestamp . '.' . str_pad((string) $microseconds, 6, '0', STR_PAD_LEFT),
        );

        if ($value === false) {
            throw new FloeException(sprintf('Floe failed to restore datetime from timestamp "%s"', $timestamp));
        }

        return $value->setTimezone($this->timeZones->get($timezoneName));
    }
}
