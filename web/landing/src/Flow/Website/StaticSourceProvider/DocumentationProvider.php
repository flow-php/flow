<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use function Flow\Filesystem\DSL\{fstab, path, protocol};
use NorbertTech\StaticContentGeneratorBundle\Content\{Source, SourceProvider};
use Symfony\Component\DependencyInjection\Attribute\Autowire;

final class DocumentationProvider implements SourceProvider
{
    public function __construct(
        #[Autowire('%documentation_base_path%')]
        private readonly string $basePath,
    ) {

    }

    public function all() : array
    {
        $files = fstab()
            ->for(protocol('file'))
            ->list(
                path($this->basePath . '/**/*.md')
            );

        foreach ($files as $file) {
            $relativePath = \str_replace(\realpath($this->basePath) . '/', '', $file->path->path());

            if (\str_starts_with($relativePath, '_')) {
                continue;
            }

            $relativePath = str_replace('.md', '', $relativePath);

            $sources[] = new Source('documentation_page', ['path' => $relativePath]);
        }

        return $sources;
    }
}
