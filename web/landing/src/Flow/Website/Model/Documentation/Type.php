<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

enum Type : string
{
    case AGGREGATING_FUNCTION = 'Aggregating Functions';
    case COMPARISON = 'Comparisons';
    case DATA_FRAME = 'Data Frame';
    case ENTRY = 'Entries';
    case EXTRACTOR = 'Extractors';
    case HELPER = 'Helpers';
    case LOADER = 'Loaders';
    case SCALAR_FUNCTION = 'Scalar Functions';
    case SCHEMA = 'Schema';
    case TRANSFORMER = 'Transformers';
    case TYPE = 'Type';
    case WINDOW_FUNCTION = 'Window Functions';

    public static function fromName(string $name) : self
    {
        $name = \mb_strtoupper(\str_replace([' ', '-'], '_', $name));

        return constant("self::{$name}");
    }

    public function priority() : int
    {
        return match ($this) {
            self::SCHEMA => 1,
            self::TYPE => 2,
            self::ENTRY => 3,
            self::DATA_FRAME => 4,
            self::EXTRACTOR => 5,
            self::TRANSFORMER => 6,
            self::AGGREGATING_FUNCTION => 7,
            self::SCALAR_FUNCTION => 8,
            self::WINDOW_FUNCTION => 9,
            self::COMPARISON => 10,
            self::HELPER => 11,
            self::LOADER => 12,
            default => 99,
        };
    }
}
