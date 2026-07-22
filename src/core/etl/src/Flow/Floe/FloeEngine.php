<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Encoder;
use Flow\ETL\Schema;

enum FloeEngine: string
{
    case adaptive = 'adaptive';
    case native = 'native';
    case php = 'php';

    /**
     * @return Encoder<string>
     */
    public function encoder(Schema $schema): Encoder
    {
        return match ($this) {
            self::adaptive => new AdaptiveFloeEncoder($schema),
            self::native => new NativeFloeEncoder($schema),
            self::php => new PhpFloeEncoder($schema),
        };
    }
}
