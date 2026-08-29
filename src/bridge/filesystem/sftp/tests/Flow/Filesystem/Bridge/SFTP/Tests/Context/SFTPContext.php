<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Context;

use Flow\Filesystem\Bridge\SFTP\Options;
use Flow\Filesystem\Bridge\SFTP\SFTPFilesystem;
use Flow\Filesystem\Bridge\SFTP\Tests\Double\RecordingSFTP;
use Flow\Filesystem\Path;
use phpseclib3\Net\SFTP;

use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_client;
use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function ltrim;
use function strrpos;
use function substr;

final readonly class SFTPContext
{
    public const BASE_DIRECTORY = '/upload';

    private SFTP $sftp;

    public function __construct()
    {
        $this->sftp = self::connect();
    }

    public static function connect(): SFTP
    {
        return sftp_client(
            type_string()->assert($_ENV['SFTP_HOST']),
            type_string()->assert($_ENV['SFTP_USER']),
            type_string()->assert($_ENV['SFTP_PASSWORD']),
            type_integer()->cast($_ENV['SFTP_PORT']),
        );
    }

    public function client(): SFTP
    {
        return $this->sftp;
    }

    public function contentOf(Path $path): string
    {
        return type_string()->assert($this->sftp->get(self::remote($path)));
    }

    public function exists(Path $path): bool
    {
        $this->sftp->clearStatCache();

        return $this->sftp->is_file(self::remote($path)) || $this->sftp->is_dir(self::remote($path));
    }

    public function filesystem(Options $options = new Options()): SFTPFilesystem
    {
        return sftp_filesystem($this->sftp, $options);
    }

    public function recordingClient(): RecordingSFTP
    {
        $sftp = new RecordingSFTP(type_string()->assert($_ENV['SFTP_HOST']), type_integer()->cast($_ENV['SFTP_PORT']));
        $sftp->login(type_string()->assert($_ENV['SFTP_USER']), type_string()->assert($_ENV['SFTP_PASSWORD']));

        return $sftp;
    }

    public function givenFileExists(Path $path, string $content): void
    {
        $remote = self::remote($path);
        $separator = strrpos($remote, '/');

        if ($separator !== false && $separator !== 0) {
            $parent = substr($remote, 0, $separator);

            if (!$this->sftp->is_dir($parent)) {
                $this->sftp->mkdir($parent, -1, true);
            }
        }

        $this->sftp->put($remote, $content, SFTP::SOURCE_STRING);
    }

    public function sizeOf(Path $path): int
    {
        $this->sftp->clearStatCache();

        return type_integer()->assert($this->sftp->filesize(self::remote($path)));
    }

    public function wipe(): void
    {
        $this->sftp->clearStatCache();

        /** @var array<array-key, string>|false $entries */
        $entries = $this->sftp->nlist(self::BASE_DIRECTORY);

        if ($entries === false) {
            return;
        }

        foreach ($entries as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }

            $this->sftp->delete(self::BASE_DIRECTORY . '/' . $entry, true);
        }
    }

    private static function remote(Path $path): string
    {
        return '/' . ltrim($path->path(), '/');
    }
}
