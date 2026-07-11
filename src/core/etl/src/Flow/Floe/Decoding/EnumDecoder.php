<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Flow\Floe\Exception\FloeException;
use UnitEnum;

use function constant;
use function defined;
use function enum_exists;
use function sprintf;
use function substr;
use function unpack;

final class EnumDecoder implements ValueDecoder
{
    public function decode(string $data, int &$position): UnitEnum
    {
        $length = unpack('V', $data, $position)[1];
        $class = substr($data, $position + 4, $length);
        $position += 4 + $length;
        $length = unpack('V', $data, $position)[1];
        $case = substr($data, $position + 4, $length);
        $position += 4 + $length;

        if (!enum_exists($class)) {
            throw new FloeException(sprintf('Floe cannot restore enum of class "%s", enum not found', $class));
        }

        if (!defined($class . '::' . $case)) {
            throw new FloeException(sprintf('Floe cannot restore enum case "%s::%s"', $class, $case));
        }

        /** @var \UnitEnum */
        return constant($class . '::' . $case);
    }
}
