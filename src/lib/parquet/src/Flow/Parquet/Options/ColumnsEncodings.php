<?php

declare(strict_types=1);

namespace Flow\Parquet\Options;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;

final class ColumnsEncodings
{
    /**
     * @param array<string, Encodings> $encodings
     */
    private function __construct(private array $encodings)
    {
    }

    /**
     * @param array<string, Encodings> $flatPathToEncodingMap
     */
    public static function create(array $flatPathToEncodingMap) : self
    {
        $encodings = [];

        foreach ($flatPathToEncodingMap as $flatPath => $encoding) {
            if (!\is_string($flatPath)) {
                throw new InvalidArgumentException('Column flat path must be a string, got: ' . \gettype($flatPath));
            }

            if ($flatPath === '') {
                throw new InvalidArgumentException('Column flat path cannot be empty');
            }

            if (!$encoding instanceof Encodings) {
                throw new InvalidArgumentException('Encoding must be an Encodings enum, got: ' . \gettype($encoding));
            }

            $encodings[$flatPath] = $encoding;
        }

        return new self($encodings);
    }

    /**
     * @param array<string, Encodings|string> $columnEncodings
     */
    public static function fromArray(array $columnEncodings) : self
    {
        $encodings = [];

        foreach ($columnEncodings as $flatPath => $encoding) {
            if (!\is_string($flatPath)) {
                throw new InvalidArgumentException('Column flat path must be a string, got: ' . \gettype($flatPath));
            }

            if ($flatPath === '') {
                throw new InvalidArgumentException('Column flat path cannot be empty');
            }

            if ($encoding instanceof Encodings) {
                $encodings[$flatPath] = $encoding;
            } elseif (\is_string($encoding)) {
                $encodings[$flatPath] = self::parseEncodingName($encoding);
            } else {
                throw new InvalidArgumentException('Encoding must be an Encodings enum or string, got: ' . \gettype($encoding));
            }
        }

        return new self($encodings);
    }

    public function count() : int
    {
        return \count($this->encodings);
    }

    public function getEncodingForFlatPath(string $flatPath) : ?Encodings
    {
        return $this->encodings[$flatPath] ?? null;
    }

    /**
     * @return array<string>
     */
    public function getFlatPaths() : array
    {
        return \array_keys($this->encodings);
    }

    public function hasFlatPath(string $flatPath) : bool
    {
        return \array_key_exists($flatPath, $this->encodings);
    }

    public function isEmpty() : bool
    {
        return empty($this->encodings);
    }

    /**
     * @return array<string, string>
     */
    public function toArray() : array
    {
        $result = [];

        foreach ($this->encodings as $flatPath => $encoding) {
            $result[$flatPath] = $encoding->name;
        }

        return $result;
    }

    private static function parseEncodingName(string $encodingName) : Encodings
    {
        return match (\strtoupper(\trim($encodingName))) {
            'PLAIN' => Encodings::PLAIN,
            'RLE_DICTIONARY' => Encodings::RLE_DICTIONARY,
            'DELTA_BINARY_PACKED' => Encodings::DELTA_BINARY_PACKED,
            default => throw new InvalidArgumentException(
                "Unsupported encoding: '{$encodingName}'. Supported encodings: PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED"
            ),
        };
    }
}
