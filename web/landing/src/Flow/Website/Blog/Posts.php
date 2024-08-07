<?php

declare(strict_types=1);

namespace Flow\Website\Blog;

final class Posts
{
    private array $posts = [
        [
            'title' => 'Building Custom Data Extractor - Flow PHP',
            'description' => 'Learn how to extract data from Google Analytics API using Flow PHP but also how to build a custom data extractor.',
            'date' => '2024-04-04',
            'slug' => 'building-custom-extractor-google-analytics',
        ],
        [
            'title' => 'Scalar Functions',
            'description' => 'Scalar functions are one of the most important building blocks of Flow. Learn how to build and use custom scalar functions in Flow PHP.',
            'date' => '2024-08-08',
            'slug' => 'scalar-functions',

        ],
    ];

    /**
     * @return array<Post>
     */
    public function all() : array
    {
        return \array_map(
            static fn (array $data) : Post => Post::fromArray($data),
            \array_reverse($this->posts)
        );
    }

    public function findByDateAndSlug(string $date, string $slug) : Post
    {
        foreach ($this->posts as $post) {
            if ($post['date'] === $date && $post['slug'] === $slug) {
                return Post::fromArray($post);
            }
        }

        throw new \InvalidArgumentException('Post not found');
    }
}
