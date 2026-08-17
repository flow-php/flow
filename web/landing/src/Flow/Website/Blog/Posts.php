<?php

declare(strict_types=1);

namespace Flow\Website\Blog;

use InvalidArgumentException;

use function array_map;
use function array_reverse;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class Posts
{
    private array $posts = [
        [
            'title' => 'Building Custom Data Extractor - Flow PHP',
            'description' => 'Learn how to extract data from Google Analytics API using Flow PHP but also how to build a custom data extractor.',
            'date' => '2024-04-04',
            'slug' => 'building-custom-extractor-google-analytics',
            'author' => 'norberttech',
        ],
        [
            'title' => 'Scalar Functions',
            'description' => 'Scalar functions are one of the most important building blocks of Flow. Learn how to build and use custom scalar functions in Flow PHP.',
            'date' => '2024-08-08',
            'slug' => 'scalar-functions',
            'author' => 'norberttech',
        ],
        [
            'title' => 'Data processing in PHP',
            'description' => 'Processing datasets is a common problem in almost any software. Learn how to process datasets in PHP just like it\'s being done in other programming languages.',
            'date' => '2025-01-25',
            'slug' => 'data-processing-in-php',
            'author' => 'norberttech',
        ],
        [
            'title' => 'Flow PHP Release Cycle',
            'description' => 'Explanation of the updated Flow PHP release cycle that improves adaptability of new features.',
            'date' => '2025-03-16',
            'slug' => 'flow-php-release-cycle',
            'author' => 'norberttech',
        ],
        [
            'title' => 'Flow PHP - Release 0.41.0',
            'description' => 'Summary of things changed in Flow PHP 0.41.0 release',
            'date' => '2026-06-29',
            'slug' => 'flow-php-release-0410',
            'author' => 'norberttech',
        ],
        [
            'title' => 'Performance Optimizations',
            'description' => 'Floe is a dedicated binary format, created to speedup serialization and reduce serialized content size of Rows.',
            'date' => '2026-07-23',
            'slug' => 'performance-optimizations',
            'author' => 'norberttech',
        ],
    ];

    /**
     * @return array<Post>
     */
    public function all(): array
    {
        return array_map(Post::fromArray(...), array_reverse($this->posts));
    }

    public function findByDateAndSlug(string $date, string $slug): Post
    {
        foreach (type_list(type_map(type_string(), type_string()))->assert($this->posts) as $post) {
            if ($post['date'] === $date && $post['slug'] === $slug) {
                return Post::fromArray($post);
            }
        }

        throw new InvalidArgumentException('Post not found');
    }
}
