<?php

declare(strict_types=1);

namespace Flow\Types\Value;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\RuntimeException;
use Ramsey\Uuid\UuidInterface;

final readonly class Uuid implements \Stringable
{
    /**
     * This regexp is a port of the Uuid library,
     * which is copyright Ben Ramsey, @see https://github.com/ramsey/uuid.
     */
    private const string UUID_REGEXP = '/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/ms';

    private string $value;

    /**
     * @throws InvalidArgumentException|RuntimeException
     */
    public function __construct(string|UuidInterface|\Symfony\Component\Uid\Uuid $value)
    {
        if (\is_string($value)) {
            try {
                if (\class_exists(\Ramsey\Uuid\Uuid::class)) {
                    $this->value = (string) \Ramsey\Uuid\Uuid::fromString($value);
                } elseif (\class_exists(\Symfony\Component\Uid\Uuid::class)) {
                    $this->value = \Symfony\Component\Uid\Uuid::fromString($value)->toRfc4122();
                } elseif (self::isValid($value)) {
                    $this->value = $value;
                } else {
                    throw new RuntimeException(
                        "\Ramsey\Uuid\Uuid nor \Symfony\Component\Uid\Uuid class not found, please add 'ramsey/uuid' or 'symfony/uid' as a dependency to the project first.",
                    );
                }
            } catch (\InvalidArgumentException $e) {
                throw new InvalidArgumentException("Invalid UUID: '{$value}'", (int) $e->getCode(), $e);
            }
        } elseif ($value instanceof UuidInterface) {
            $this->value = $value->toString();
        } else {
            $this->value = $value->toRfc4122();
        }
    }

    public static function fromString(string $value): self
    {
        return new self($value);
    }

    public static function isValid(string $value): bool
    {
        if (\strlen($value) !== 36) {
            return false;
        }

        return 1 === \preg_match(self::UUID_REGEXP, $value);
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public function isEqual(self $type): bool
    {
        return $this->toString() === $type->toString();
    }

    public function toString(): string
    {
        return $this->value;
    }
}
