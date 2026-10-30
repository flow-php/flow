<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Describe;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use JsonException;

use function is_array;
use function json_decode;
use function json_encode;

use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;

final readonly class Json
{
    public const int FLAGS = JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_UNESCAPED_SLASHES;

    /**
     * @throws InvalidArgumentException when the document is not a JSON array or object
     * @throws JsonException
     *
     * @return array<array-key, mixed>
     */
    public static function decode(string $json): array
    {
        // @mago-ignore analysis:mixed-assignment
        $decoded = json_decode($json, true, 512, self::FLAGS);

        if (!is_array($decoded)) {
            throw new InvalidArgumentException('Expected a JSON array or object');
        }

        return $decoded;
    }

    /**
     * @param array<array-key, mixed> $data
     *
     * @throws JsonException
     */
    public static function encode(array $data): string
    {
        $json = json_encode($data, self::FLAGS);

        if ($json === false) {
            throw new JsonException('Failed to encode');
        }

        return $json;
    }
}
