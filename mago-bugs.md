# Mago Analyzer Bugs

This document tracks potential bugs found in the Mago PHP static analyzer.

## Bug 1: False positive for unused-template-parameter when used in @implements

**File:** `src/lib/types/src/Flow/Types/Type/Logical/ClassStringType.php:16:22`

**Error:** `warning[unused-template-parameter]: Template parameter `T` is never used in class `Flow\Types\Type\Logical\ClassStringType`.`

**Code:**
```php
/**
 * @template T of object
 *
 * @implements Type<class-string<T>>
 */
final readonly class ClassStringType implements Type
{
    /**
     * @param null|class-string<T> $class
     */
    public function __construct(
        public null|string $class = null,
    ) {
```

**Why it's a bug:**
The template parameter `T` is clearly used in two places:
1. `@implements Type<class-string<T>>` - The class implements `Type` with a generic parameter that uses `T`
2. `@param null|class-string<T> $class` - The constructor parameter uses `T`

Mago incorrectly reports this template parameter as unused when it's being used in the `@implements` annotation to specify the type parameter for the implemented interface.

---

## Bug 2: False positive never-return in conditional control flow

**File:** `src/lib/types/src/Flow/Types/Type/Native/UnionType.php:78:20`

**Error:** `error[never-return]: Cannot return value with type 'never' from this function.`

**Code:**
```php
#[\Override]
public function assert(mixed $value): mixed
{
    if ($this->left->isValid($value)) {
        return $value;
    }

    if ($this->right->isValid($value)) {
        return $value;  // ERROR: Mago claims $value is 'never' here
    }

    throw InvalidTypeException::value($value, $this);
}
```

**Why it's a bug:**
Mago incorrectly infers that `$value` has type `never` at line 78. This is incorrect control flow analysis. The function receives `mixed $value` as a parameter, and if the first condition is false, the control flows to the second condition. The value `$value` should still be of type `mixed` (or narrowed based on the `isValid` check), not `never`.

The `never` type should only appear when no value is possible (e.g., after a function that always throws), but here `$value` is simply a parameter that hasn't been modified.

---

## Bug 3: False positive never type in sequential conditional checks

**File:** `src/lib/types/src/Flow/Types/Type/TypeDetector.php:50:38`

**Error:** `error[no-value]: Argument #1 passed to method `Flow\Types\Type::isvalid` has type `never`, meaning it cannot produce a value.`

**Code:**
```php
if (\is_string($value)) {
    if (type_json()->isValid($value)) {
        return type_json();
    }

    if (type_uuid()->isValid($value)) {  // ERROR: $value is 'never'
        return type_uuid();
    }

    return type_string();
}
```

**Why it's a bug:**
Mago incorrectly infers `$value` as `never` at line 50. After the outer `if (\is_string($value))` check and the inner `type_json()->isValid($value)` check (which returns false), `$value` should still be typed as `string`, not `never`. The control flow analysis is incorrectly propagating "never" through sequential checks.

---

## Bug 4: False positive impossible-type-comparison for array_is_list

**File:** `src/lib/types/src/Flow/Types/Type/TypeDetector.php:77:17`

**Error:** `error[impossible-type-comparison]: Impossible type assertion: `$value` of type `non-empty-array<array-key, mixed>` can never be `list<mixed>`.`

**Code:**
```php
if (\is_array($value)) {
    if ([] === $value) {
        return type_array();
    }

    $detector = new ArrayContentDetector(
        types(...\array_map($this->detectType(...), \array_keys($value)))->deduplicate(),
        types(...\array_map($this->detectType(...), \array_values($value)))->deduplicate(),
        \array_is_list($value),  // ERROR: "impossible" comparison
    );
```

**Why it's a bug:**
Mago claims that a `non-empty-array<array-key, mixed>` can never be a `list<mixed>`, but this is incorrect. A list is a subset of arrays - any array with sequential integer keys starting from 0 is a list. For example, `[1, 2, 3]` is both a `non-empty-array<int, int>` and a `list<int>`. The `array_is_list()` function is specifically designed to check this condition.

---

## Summary

**Starting point:** 56 issues (30 errors, 25 warnings, 1 help)
**Final result:** 15 issues (3 errors, 12 warnings)

### Errors (3 total - all false positives documented above)
- Bug 2: UnionType.php:78 - `never-return`
- Bug 3: TypeDetector.php:50 - `no-value`
- Bug 4: TypeDetector.php:79 - `impossible-type-comparison`

### Warnings (12 total)

The remaining warnings are mostly intentional `mixed` usage in type casting and detection code, which is the core purpose of this library:

| File | Warnings | Reason |
|------|----------|--------|
| ListType.php | 2 | Iterating array<mixed> values (inherent) |
| MapType.php | 3 | Iterating iterable<mixed> values (inherent) |
| AutoCaster.php | 2 | Iterating array<mixed> values (inherent) |
| TypeDetector.php | 1 | Detecting types from mixed values (inherent) |
| FloatType.php | 1 | String to float cast - intentional fallback |
| BooleanType.php | 1 | Mixed to bool cast - intentional fallback |
| ClassStringType.php | 1 | Bug 1 - false positive for template parameter |
| EnumType.php | 1 | `possibly-static-access-on-interface` - limitation, not bug |

### Fixes Applied

The following issues were fixed during this analysis:

1. **XMLConverter.php** - Removed redundant docblock
2. **DateType.php, DateTimeType.php** - Cast bool to int in string concatenation
3. **StringType.php, NonEmptyStringType.php** - Fixed object-to-string type narrowing
4. **EnumType.php** - Added type annotations for BackedEnum
5. **StringTypeNarrower.php** - Fixed mixed-operand errors
6. **Comparator.php** - Added type annotations for generics
7. **HTMLType.php** - Fixed logic bug in cast() method
8. **TypeFactory.php** - Simplified error message
9. **TypeDetector.php** - Added null checks and type annotations
10. **StructureType.php** - Added type annotations for array elements
11. **Types.php** - Added null check for first() result
12. **Uuid.php** - Cast exception code to int
13. **UuidType.php** - Added type narrowing annotations
14. **MapType.php** - Added type annotations for key type and json_decode
15. **FloatType.php** - Added numeric-string annotations for format() calls
16. **ArrayType.php** - Added type annotations for json_decode and explicit null check
