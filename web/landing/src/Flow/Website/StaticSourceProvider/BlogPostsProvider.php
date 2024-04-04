<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use NorbertTech\StaticContentGeneratorBundle\Content\{Source, SourceProvider};

final class BlogPostsProvider implements SourceProvider
{
    public function __construct()
    {

    }

    public function all() : array
    {
        $sources = [];

        $sources[] = new Source('blog_post', ['date' => '2024-04-04', 'slug' => 'building-custom-extractor-google-analytics']);

        return $sources;
    }
}
