<?php

declare(strict_types=1);

namespace Flow\Types\Value;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\RuntimeException;
use Ramsey\Uuid\UuidInterface;
use ReflectionClass;
use Stringable;
use Symfony\Component\Uid\Uuid as SymfonyUuid;

use function bin2hex;
use function is_string;
use function preg_match;
use function sprintf;
use function strlen;
use function substr;

final readonly class Uuid implements Stringable
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
    public function __construct(string|UuidInterface|SymfonyUuid $value)
    {
        if (is_string($value)) {
            if (self::isValid($value)) {
                $this->value = $value;
            } else {
                throw new InvalidArgumentException("Invalid UUID: '{$value}'");
            }
        } elseif ($value instanceof UuidInterface) {
            $this->value = $value->toString();
        } else {
            $this->value = $value->toRfc4122();
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromBytes(string $bytes): self
    {
        if (strlen($bytes) !== 16) {
            throw new InvalidArgumentException('Uuid::fromBytes() expects 16 bytes, got ' . strlen($bytes));
        }

        $hex = bin2hex($bytes);
        $uuid = (new ReflectionClass(self::class))->newInstanceWithoutConstructor();
        // @mago-ignore analysis:invalid-property-write
        $uuid->value = sprintf(
            '%s-%s-%s-%s-%s',
            substr($hex, 0, 8),
            substr($hex, 8, 4),
            substr($hex, 12, 4),
            substr($hex, 16, 4),
            substr($hex, 20),
        );

        return $uuid;
    }

    public static function fromString(string $value): self
    {
        return new self($value);
    }

    public static function isValid(string $value): bool
    {
        if (strlen($value) !== 36) {
            return false;
        }

        return 1 === preg_match(self::UUID_REGEXP, $value);
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
