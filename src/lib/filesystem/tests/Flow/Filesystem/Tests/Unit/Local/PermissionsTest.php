<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Local;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Local\Permissions;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class PermissionsTest extends TestCase
{
    #[TestWith([0, 0])]
    #[TestWith([0o777, 0o777])]
    public function test_accepts_modes_within_range(int $file, int $directory): void
    {
        $permissions = new Permissions($file, $directory);

        static::assertSame($file, $permissions->file);
        static::assertSame($directory, $permissions->directory);
    }

    public function test_normalize_round_trips(): void
    {
        $permissions = new Permissions(0o640, 0o750);

        static::assertSame(['file' => 0o640, 'directory' => 0o750], $permissions->normalize());
        static::assertEquals($permissions, Permissions::fromArray($permissions->normalize()));
    }

    #[TestWith([['file' => 0o600]])]
    #[TestWith([['file' => '0600', 'directory' => 0o700]])]
    public function test_from_array_refuses_an_invalid_shape(array $data): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Permissions must hold file (int) and directory (int)');

        Permissions::fromArray($data);
    }

    public function test_private_is_owner_only(): void
    {
        $permissions = Permissions::private();

        static::assertSame(0o600, $permissions->file);
        static::assertSame(0o700, $permissions->directory);
    }

    #[TestWith([-1, 0o700, 'Permissions file mode must be between 0 and 0777 (511), got -1'])]
    #[TestWith([0o1000, 0o700, 'Permissions file mode must be between 0 and 0777 (511), got 512'])]
    #[TestWith([0o600, -1, 'Permissions directory mode must be between 0 and 0777 (511), got -1'])]
    #[TestWith([0o600, 0o1000, 'Permissions directory mode must be between 0 and 0777 (511), got 512'])]
    public function test_rejects_modes_out_of_range(int $file, int $directory, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        new Permissions($file, $directory);
    }
}
