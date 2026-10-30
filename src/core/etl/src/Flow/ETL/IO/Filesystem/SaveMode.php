<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Filesystem;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

enum SaveMode: string
{
    /**
     * If data sink already exists, data will be appended, this solution might cause data duplication
     * since it's not check if given rows already existed.
     */
    case Append = 'append';
    /**
     * If data sink already exists error will be thrown.
     */
    case ExceptionIfExists = 'exception_if_exists';

    /**
     * If data sink already exists, writing will be skipped.
     */
    case Ignore = 'ignore';

    /**
     * If data sink already exists, it will be removed and written again.
     */
    case Overwrite = 'overwrite';

    /**
     * Whether a write into $glob goes ahead, decided once for the whole destination: Ignore over an existing file
     * answers false, ExceptionIfExists over an existing file throws, Append beside a regular file at the static root
     * of a pattern throws. Hidden paths below the static root (another write's temporary output) are not files of
     * the destination.
     *
     * @throws RuntimeException
     */
    public function admits(Filesystem $filesystem, Path $glob): bool
    {
        if (
            $this === self::Append
            && $glob->isPattern()
            && $filesystem->status($glob->staticPart())?->isFile() === true
        ) {
            throw new RuntimeException(
                'Destination path "' . $glob->staticPart()->uri() . '" is a file, parts cannot be appended beside it',
            );
        }

        if ($this !== self::ExceptionIfExists && $this !== self::Ignore) {
            return true;
        }

        $exists = false;

        if (!$glob->isPattern()) {
            $exists = $filesystem->status($glob) !== null;
        } else {
            foreach ($filesystem->list($glob, new VisibleFiles($glob->staticPart())) as $_) {
                $exists = true;

                break;
            }
        }

        if ($exists && $this === self::ExceptionIfExists) {
            throw new RuntimeException(
                'Destination path "'
                . $glob->uri()
                . '" already exists, please change path to different or set different SaveMode',
            );
        }

        return !$exists;
    }

    public function replaces(): bool
    {
        return $this === self::Overwrite;
    }
}
