<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\Exception\InvalidArgumentException;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_structure;
use function sprintf;

/**
 * @type PermissionsShape = array{file: int, directory: int}
 */
final readonly class Permissions
{
    public function __construct(
        public int $file,
        public int $directory,
    ) {
        foreach (['file' => $file, 'directory' => $directory] as $name => $mode) {
            if ($mode < 0 || $mode > 0o777) {
                throw new InvalidArgumentException(sprintf(
                    'Permissions %s mode must be between 0 and 0777 (511), got %d',
                    $name,
                    $mode,
                ));
            }
        }
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        if (!type_structure(['file' => type_integer(), 'directory' => type_integer()])->isValid($data)) {
            throw new InvalidArgumentException('Permissions must hold file (int) and directory (int)');
        }

        return new self($data['file'], $data['directory']);
    }

    public static function private(): self
    {
        return new self(0o600, 0o700);
    }

    /**
     * @return PermissionsShape
     */
    public function normalize(): array
    {
        return ['file' => $this->file, 'directory' => $this->directory];
    }
}
