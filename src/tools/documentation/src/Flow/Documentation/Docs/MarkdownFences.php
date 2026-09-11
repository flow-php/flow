<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use RuntimeException;

use function explode;
use function file_get_contents;
use function implode;
use function sprintf;
use function str_starts_with;
use function substr;

final readonly class MarkdownFences
{
    /**
     * @return list<Fence>
     */
    public function of(string $file): array
    {
        $contents = file_get_contents($file);

        if ($contents === false) {
            throw new RuntimeException(sprintf('Unable to read "%s".', $file));
        }

        $fences = [];
        $openedAt = null;
        $info = FenceInfo::parse('');
        $body = [];

        foreach (explode("\n", $contents) as $index => $line) {
            if (!str_starts_with($line, '```')) {
                if ($openedAt !== null) {
                    $body[] = $line;
                }

                continue;
            }

            if ($openedAt === null) {
                $openedAt = $index + 1;
                $info = FenceInfo::parse(substr($line, 3));
                $body = [];

                continue;
            }

            $fences[] = new Fence($file, $openedAt, $info, implode("\n", $body));
            $openedAt = null;
            $info = FenceInfo::parse('');
            $body = [];
        }

        return $fences;
    }

    /**
     * @return list<Fence>
     */
    public function phpOf(string $file): array
    {
        $php = [];

        foreach ($this->of($file) as $fence) {
            if ($fence->info->isPhp()) {
                $php[] = $fence;
            }
        }

        return $php;
    }
}
