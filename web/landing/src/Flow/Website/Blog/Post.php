<?php

declare(strict_types=1);

namespace Flow\Website\Blog;

final class Post
{
    public function __construct(
        public readonly string $title,
        public readonly string $description,
        public readonly \DateTimeImmutable $date,
        public readonly string $slug
    ) {
    }

    public static function fromArray(array $data) : self
    {
        return new self(
            $data['title'],
            $data['description'],
            new \DateTimeImmutable($data['date']),
            $data['slug']
        );
    }
}
