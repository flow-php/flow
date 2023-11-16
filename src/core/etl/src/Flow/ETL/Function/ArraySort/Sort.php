<?php declare(strict_types=1);

namespace Flow\ETL\Function\ArraySort;

use Flow\ETL\Exception\InvalidArgumentException;

enum Sort : string
{
    case arsort = 'arsort';
    case asort = 'asort';
    case krsort = 'krsort';
    case ksort = 'ksort';
    case natcasesort = 'natcasesort';
    case natsort = 'natsort';
    case rsort = 'rsort';
    case shuffle = 'shuffle';
    case sort = 'sort';

    public static function fromString(string $value) : self
    {
        $value = \strtolower($value);

        foreach (self::cases() as $case) {
            if ($value === $case->name) {
                return $case;
            }
        }

        throw new InvalidArgumentException("Unsupported sort method: {$value}");
    }
}
