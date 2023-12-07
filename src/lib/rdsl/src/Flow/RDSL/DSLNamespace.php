<?php declare(strict_types=1);

namespace Flow\RDSL;

use Flow\RDSL\AccessControl\AllowAll;
use Flow\RDSL\Exception\InvalidArgumentException;

final class DSLNamespace
{
    private const NAMESPACE_REGEX = '/^\\\\[a-zA-Z_][a-zA-Z0-9_]*(\\\\[a-zA-Z_][a-zA-Z0-9_]*)*$/';

    public function __construct(
        public readonly string $name,
        private readonly AccessControl $acl = new AllowAll()
    ) {
        if ($name !== '\\' && !\preg_match(self::NAMESPACE_REGEX, $name)) {
            throw new InvalidArgumentException(\sprintf('Namespace name "%s" is invalid.', $name));
        }
    }

    public static function global(AccessControl $acl = new AllowAll()) : self
    {
        return new self('\\', $acl);
    }

    public function isAllowed(string $name) : bool
    {
        return $this->acl->isAllowed($name);
    }

    public function isEqual(self|string $namespace) : bool
    {
        if (\is_string($namespace)) {
            $ns = '\\' . \trim($namespace, '\\');
        } else {
            $ns = $namespace->name;
        }

        return \mb_strtolower($this->name) === \mb_strtolower($ns);
    }
}
