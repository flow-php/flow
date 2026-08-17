<?php

declare(strict_types=1);

namespace Flow\Website\Blog;

use DateTimeImmutable;

use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function sprintf;

final readonly class Post
{
    public function __construct(
        public string $title,
        public string $description,
        public DateTimeImmutable $date,
        public string $slug,
        public string $author,
    ) {}

    public static function fromArray(array $data): self
    {
        type_structure([
            'title' => type_string(),
            'description' => type_string(),
            'date' => type_string(),
            'slug' => type_string(),
            'author' => type_string(),
        ])->assert($data);

        return new self(
            $data['title'],
            $data['description'],
            new DateTimeImmutable($data['date']),
            $data['slug'],
            $data['author'],
        );
    }

    public function authorAvatarUrl(int $size = 96): string
    {
        return sprintf('https://github.com/%s.png?size=%d', $this->author, $size);
    }

    public function authorUrl(): string
    {
        return 'https://github.com/' . $this->author;
    }
}
