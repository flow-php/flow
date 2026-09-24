<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Schema;

enum FloeEngine: string
{
    case adaptive = 'adaptive';
    case native = 'native';
    case php = 'php';

    public function encoder(Schema $schema): FloeEncoder
    {
        return match ($this) {
            self::adaptive => NativeFloeEncoder::isSupported()
                ? new NativeFloeEncoder($schema)
                : new PhpFloeEncoder($schema),
            self::native => new NativeFloeEncoder($schema),
            self::php => new PhpFloeEncoder($schema),
        };
    }
}
