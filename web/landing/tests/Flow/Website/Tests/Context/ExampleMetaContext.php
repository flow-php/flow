<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Context;

use Flow\Website\Service\Examples\ExampleMeta;

use function file_put_contents;
use function is_dir;
use function mkdir;
use function rmdir;
use function sys_get_temp_dir;
use function uniqid;
use function unlink;

final class ExampleMetaContext
{
    private string $directory = '';

    public function clean(): void
    {
        if ($this->directory === '') {
            return;
        }

        @unlink($this->directory . '/_meta.yaml');
        @rmdir($this->directory);
        $this->directory = '';
    }

    public function directoryWithout(): string
    {
        $this->make();

        return $this->directory;
    }

    public function parse(string $yaml): ExampleMeta
    {
        $this->make();
        file_put_contents($this->directory . '/_meta.yaml', $yaml);

        return ExampleMeta::fromDirectory($this->directory);
    }

    private function make(): void
    {
        if ($this->directory === '') {
            $this->directory = sys_get_temp_dir() . '/flow-meta-' . uniqid('', true);
        }

        if (!is_dir($this->directory)) {
            mkdir($this->directory, 0o777, true);
        }
    }
}
