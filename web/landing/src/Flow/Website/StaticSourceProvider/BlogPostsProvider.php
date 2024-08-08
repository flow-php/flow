<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Flow\Website\Blog\Posts;
use NorbertTech\StaticContentGeneratorBundle\Content\{Source, SourceProvider};

final class BlogPostsProvider implements SourceProvider
{
    public function __construct()
    {

    }

    public function all() : array
    {
        $sources = [];

        foreach ((new Posts())->all() as $post) {
            $sources[] = new Source('blog_post', ['date' => $post->date->format('Y-m-d'), 'slug' => $post->slug]);
        }

        return $sources;
    }
}
