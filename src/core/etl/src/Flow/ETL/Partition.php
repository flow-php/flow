<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{name: string, value: string}>
 */
final class Partition implements Serializable
{
    /**
     * @var array<string>
     */
    private static array $forbiddenCharacters = ['/', '\\', '=', ':', '>', '<', '|', '"', '?', '*'];

    public function __construct(public readonly string $name, public readonly string $value)
    {
        if (!\strlen($this->name)) {
            throw new InvalidArgumentException("Partition name can't be empty");
        }

        if (!\strlen($this->value)) {
            throw new InvalidArgumentException("Partition value can't be empty");
        }

        $regex = '/^([^\/\\\=:><|"?*]+)$/';

        if (!\preg_match($regex, $this->name)) {
            throw new InvalidArgumentException("Partition name contains one of forbidden characters: ['" . \implode("', '", self::$forbiddenCharacters) . "']");
        }

        if (!\preg_match($regex, $this->value)) {
            throw new InvalidArgumentException("Partition value contains one of forbidden characters: ['" . \implode("', '", self::$forbiddenCharacters) . "']");
        }
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return array<Partition>
     *
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress MixedAssignment
     */
    public static function fromArray(array $data) : array
    {
        $partitions = [];

        foreach ($data as $partition => $value) {
            /** @phpstan-ignore-next-line */
            $partitions[] = new self($partition, (string) $value);
        }

        return $partitions;
    }

    /**
     * @return array<Partition>
     */
    public static function fromUri(string $uri) : array
    {
        $regex = '/^([^\/\\\=:><|"?*]+)=([^\/\\\=:><|"?*]+)$/';

        $partitions = [];

        foreach (\array_filter(\explode('/', $uri), 'strlen') as $uriPart) {
            if (\preg_match($regex, $uriPart, $matches)) {
                $partitions[] = new self($matches[1], $matches[2]);
            }
        }

        return $partitions;
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
    }
}
