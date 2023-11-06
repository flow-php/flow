<?php declare(strict_types=1);

namespace Flow\ETL\Test;

trait FilesystemTestHelper
{
    protected function cleanDirectory(string $path) : void
    {
        if (\is_dir($path)) {
            $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

            foreach ($files as $file) {
                if (\is_file($path . DIRECTORY_SEPARATOR . $file)) {
                    $this->removeFile($path . DIRECTORY_SEPARATOR . $file);
                } else {
                    $this->cleanDirectory($path . DIRECTORY_SEPARATOR . $file);
                }
            }

            \rmdir($path);
        }
    }

    protected function createTemporaryFile(string $prefix, string $extension) : string
    {
        $path = \sys_get_temp_dir() . '/test-flow-' . \crc32(static::class) . '-' . \strtr('.', '', \uniqid($prefix, true)) . $extension;
        $this->removeFile($path);

        return $path;
    }

    protected function listDirectoryFiles(string $path) : array
    {
        return \array_values(\array_diff(\scandir($path), ['.', '..']));
    }

    /**
     * @param string $path
     */
    protected function removeFile(string $path) : void
    {
        if (\file_exists($path)) {
            if (\is_dir($path)) {
                $this->cleanDirectory($path);
            } else {
                \unlink($path);
            }
        }
    }
}
