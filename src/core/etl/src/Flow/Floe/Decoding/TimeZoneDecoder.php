<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use DateTimeZone;

use function substr;
use function unpack;

final class TimeZoneDecoder implements ValueDecoder
{
    public function __construct(
        private readonly TimeZones $timeZones,
    ) {}

    public function decode(string $data, int &$position): DateTimeZone
    {
        $length = unpack('V', $data, $position)[1];
        $name = substr($data, $position + 4, $length);
        $position += 4 + $length;

        return $this->timeZones->get($name);
    }
}
