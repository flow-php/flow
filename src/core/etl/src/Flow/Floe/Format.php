<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\Floe\Exception\FloeException;

use function chr;
use function ord;
use function pack;
use function sprintf;
use function str_starts_with;
use function strlen;
use function substr;
use function unpack;

final class Format
{
    public const string MAGIC = 'FLOE';

    public const int VERSION = 0x01;

    public const int HEADER_LENGTH = 6;

    public const int TRAILER_LENGTH = 8;

    public const int FRAME_HEADER_LENGTH = 5;

    public const int FRAME_SCHEMA = 0x01;

    public const int FRAME_ROW = 0x02;

    public const int FRAME_PARTITIONS = 0x03;

    public const int FRAME_FOOTER = 0x06;

    public const int VALUE_NULL = 0x00;

    public const int VALUE_PRESENT = 0x01;

    public const int VALUE_NULL_FROM_NULL = 0x02;

    public const int VALUE_ABSENT = 0x03;

    public const string VALUE_NULL_BYTE = "\x00";

    public const string VALUE_PRESENT_BYTE = "\x01";

    public const string VALUE_NULL_FROM_NULL_BYTE = "\x02";

    public const string VALUE_ABSENT_BYTE = "\x03";

    public const int DATETIME_IMMUTABLE = 0x00;

    public const int DATETIME_MUTABLE = 0x01;

    public const int TAG_NULL = 0x00;

    public const int TAG_INTEGER = 0x01;

    public const int TAG_FLOAT = 0x02;

    public const int TAG_BOOLEAN = 0x03;

    public const int TAG_STRING = 0x04;

    public const int TAG_ARRAY = 0x05;

    public const int TAG_DATETIME = 0x06;

    public const int TAG_UUID = 0x07;

    public const int TAG_JSON = 0x08;

    public const int KEY_INTEGER = 0x00;

    public const int KEY_STRING = 0x01;

    public static function frame(int $type, string $body): string
    {
        return chr($type) . pack('V', strlen($body)) . $body;
    }

    public static function header(int $flags): string
    {
        return self::MAGIC . chr(self::VERSION) . chr($flags);
    }

    /**
     * @throws FloeException
     *
     * @return int footer JSON length
     */
    public static function parseTrailer(string $bytes): int
    {
        if (strlen($bytes) !== self::TRAILER_LENGTH) {
            throw new FloeException('Floe trailer must be exactly 8 bytes, file is torn or not a Floe file');
        }

        if (substr($bytes, 4) !== self::MAGIC) {
            throw new FloeException(
                'Floe trailer does not end with expected magic bytes, file is torn or not a Floe file',
            );
        }

        return unpack('V', $bytes)[1];
    }

    public static function trailer(int $footerLength): string
    {
        return pack('V', $footerLength) . self::MAGIC;
    }

    /**
     * @throws FloeException
     *
     * @return int header flags byte
     */
    public static function validateHeader(string $bytes): int
    {
        if (strlen($bytes) < self::HEADER_LENGTH) {
            throw new FloeException('Floe data is truncated, header is incomplete');
        }

        if (!str_starts_with($bytes, self::MAGIC)) {
            throw new FloeException('Floe data does not start with expected magic bytes');
        }

        $version = ord($bytes[4]);

        if ($version !== self::VERSION) {
            throw new FloeException(sprintf('Floe does not support format version %d', $version));
        }

        return ord($bytes[5]);
    }

    /**
     * @throws FloeException
     */
    public static function validateCodecId(int $codecId): void
    {
        if ($codecId !== 0x00) {
            throw new FloeException(sprintf(
                'Floe format v1 supports only the no-op codec, got codec 0x%02X',
                $codecId,
            ));
        }
    }
}
