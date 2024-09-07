There are multiple ways to rename entries in a DataFrame:

- `rename(string $from, string $to)` - renames a single entry
- `renameAll(string $search, string $replace)` - renames all entries that contain a given substring and replaces it with another substring
- `renameAllToLowercase()` - renames all entries to lowercase
- `renameAllStyle(StringStyles|string $style)` - renames all entries to a given style (e.g. camel, snakem, kebab, etc.)
