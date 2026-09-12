<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\DirectoryTraversal;

use function Flow\Types\DSL\type_integer;

enum RemoteEntryType: int
{
    case BLOCK_DEVICE = 8;
    case CHAR_DEVICE = 7;
    case DIRECTORY = 2;
    case FIFO = 9;
    case REGULAR = 1;
    case SOCKET = 6;
    case SPECIAL = 4;
    case SYMLINK = 3;
    case UNKNOWN = 5;

    public static function fromRawType(mixed $type): self
    {
        if (!type_integer()->isValid($type)) {
            return self::UNKNOWN;
        }

        return self::tryFrom($type) ?? self::UNKNOWN;
    }
}
